package http

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/balits/kave/internal/peer"
	"github.com/balits/kave/internal/service"
	"github.com/balits/kave/internal/transport"
)

const (
	errMsgReadMiddleware  = "read middleware error"
	errMsgWriteMiddleware = "write middleware error"
	errMsgProxyLeader     = "proxying to leader failed"

	authErrMsg = "failed to authenticate"
)

var (
	errAuthTokenMismatch = errors.New("auth token missmatch")
	errAuthTokenNotFound = errors.New("auth token not found")
)

type middleware = func(http.Handler) http.Handler

func chain(base http.Handler, ms ...middleware) http.Handler {
	for _, m := range ms {
		base = m(base)
	}
	return base
}

func (s *HttpServer) writeChain(base http.HandlerFunc) http.Handler {
	return chain(base, s.requestLoggingMiddleware, s.writeLimitMiddleware, s.leaderMiddleware)
}

func (s *HttpServer) strongReadChain(base http.HandlerFunc) http.Handler {
	return chain(base, s.requestLoggingMiddleware, s.readLimitMiddleware, s.consitencyMiddleware)
}

// weakReadChain does not contain the consistentcy middleware in the chain,
// making reads return possible stale state (fine for OtInit for example)
func (s *HttpServer) weakReadChain(base http.HandlerFunc) http.Handler {
	return chain(base, s.requestLoggingMiddleware, s.readLimitMiddleware)
}

func (s *HttpServer) adminChain(base http.HandlerFunc) http.Handler {
	// adminAuthMiddleware before writeLimitMiddleware:	dont waste write tokens if the auth header is invalid
	return chain(base, s.requestLoggingMiddleware, s.adminAuthMiddleware, s.writeLimitMiddleware, s.leaderMiddleware)
}

func (s *HttpServer) consitencyMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		drainedBytes, err := drainBody(r.Body)
		if err != nil {
			s.writeError(w, errMsgReadMiddleware, err, http.StatusBadRequest)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(drainedBytes))

		var peek struct {
			Serializable bool `json:"serializable"`
		}

		if err := json.Unmarshal(drainedBytes, &peek); err != nil {
			s.writeError(w, errMsgReadMiddleware, err, http.StatusBadRequest)
			return
		}

		if peek.Serializable {
			next.ServeHTTP(w, r)
			return
		}

		leader, err := s.raftSvc.Leader(r.Context())
		if err != nil {
			s.writeError(w, errMsgReadMiddleware, err, http.StatusServiceUnavailable)
			return
		}

		if leader.NodeID != s.me.NodeID {
			s.proxyToLeader(w, r, leader)
			return
		}

		if err := s.raftSvc.VerifyLeader(r.Context()); err != nil {
			s.writeError(w, errMsgReadMiddleware, err, http.StatusServiceUnavailable)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func drainBody(oldBody io.ReadCloser) (read []byte, err error) {
	defer func() {
		_ = oldBody.Close()
	}()

	read, err = io.ReadAll(oldBody)
	if err != nil {
		return nil, fmt.Errorf("draining body failed: %w", err)
	}
	return
}

func (s *HttpServer) leaderMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		leader, err := s.raftSvc.Leader(r.Context())
		if err != nil {
			s.writeError(w, errMsgWriteMiddleware, err, http.StatusServiceUnavailable)
			return
		}

		if leader.NodeID != s.me.NodeID {
			s.proxyToLeader(w, r, leader)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func newReverseProxy(advertisedAddr string) *httputil.ReverseProxy {
	target := &url.URL{
		Scheme: "http",
		Host:   advertisedAddr,
	}
	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
		},
		ModifyResponse: func(resp *http.Response) error {
			// corsMuxMiddleware on this node will set them correctly,
			// delete them to avoid duplicate erros on the frontend
			resp.Header.Del("Access-Control-Allow-Origin")
			resp.Header.Del("Access-Control-Allow-Methods")
			resp.Header.Del("Access-Control-Allow-Headers")
			return nil
		},
	}
}

func (s *HttpServer) proxyToLeader(w http.ResponseWriter, r *http.Request, leader peer.Peer) {
	proxy := newReverseProxy(leader.GetHttpAdvertisedAddress())
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		if errors.Is(err, service.ErrLeaderNotFound) || networkerr(err) {
			s.writeError(w, errMsgProxyLeader, err, http.StatusServiceUnavailable)
			return
		}

		s.writeError(w, errMsgProxyLeader, err, http.StatusBadGateway,
			"leader_id", leader.NodeID,
			"leader_addr", leader.GetHttpAdvertisedAddress(),
		)
	}

	s.logger.Debug("proxying request to leader",
		"leader_id", leader.NodeID,
		"path", r.URL.Path,
	)
	proxy.ServeHTTP(w, r)
}

func (s *HttpServer) requestLoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		s.logger.WithGroup("request").
			Debug("new request",
				"method", r.Method,
				"url", r.URL.Path,
				"start_time", start,
			)

		i := newStatusCodeInterceptor(w)
		next.ServeHTTP(i, r)

		s.logger.WithGroup("request").
			Debug("request finished",
				"method", r.Method,
				"url", r.URL.Path,
				"elapsed_time", time.Since(start),
				"status_code", i.statusCode,
			)
	})
}

// corsMuxMiddleware sets general CORS headers on the servers Multiplexer,
// instead of sprinkling it on individual routes.
func (s *HttpServer) corsMuxMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers",
			"Content-Type"+", "+
				transport.AdminAuthTokenHeaderName,
		)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *HttpServer) panicRecoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				s.logger.Error("HTTP request panicked",
					"error", err,
					"path", r.URL.Path,
					"method", r.Method,
				)

				s.writeError(w, "Internal Server Error", fmt.Errorf("unexpected error (panicked): %v", err), http.StatusInternalServerError)
			}
		}()

		next.ServeHTTP(w, r)
	})
}

func (s *HttpServer) adminAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestToken := r.Header.Get(transport.AdminAuthTokenHeaderName)

		if len(requestToken) == 0 {
			s.writeError(w, authErrMsg, errAuthTokenNotFound, http.StatusForbidden)
			return
		}

		if requestToken != s._adminAuthToken {
			s.writeError(w, authErrMsg, errAuthTokenMismatch, http.StatusForbidden)
			return
		}

		next.ServeHTTP(w, r)
	})
}

type statusCodeInterceptor struct {
	w          http.ResponseWriter
	statusCode int
}

func newStatusCodeInterceptor(w http.ResponseWriter) *statusCodeInterceptor {
	return &statusCodeInterceptor{
		w:          w,
		statusCode: http.StatusOK,
	}
}

func (i *statusCodeInterceptor) Header() http.Header {
	return i.w.Header()
}

func (i *statusCodeInterceptor) Write(bb []byte) (int, error) {
	return i.w.Write(bb)
}

func (i *statusCodeInterceptor) WriteHeader(statusCode int) {
	i.statusCode = statusCode
	i.w.WriteHeader(statusCode)
}

func (i *statusCodeInterceptor) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := i.w.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("webserver does not support hijacking")
	}
	return h.Hijack()
}

func networkerr(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true // timeout or dial errors
	}
	str := err.Error()
	return strings.Contains(str, "connection refused") || strings.Contains(str, "EOF")
}
