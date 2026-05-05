package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/balits/kave/internal/lease"
	"github.com/balits/kave/internal/mvcc"
	"github.com/balits/kave/internal/peer"
	"github.com/balits/kave/internal/service"
	"github.com/balits/kave/internal/transport"
	"github.com/balits/kave/internal/types/api"
	"github.com/balits/kave/internal/util"
	"github.com/balits/kave/internal/watch"
	"github.com/coder/websocket"
	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	RouteKvRange  = transport.RouteKv + "/range"
	RouteKvPut    = transport.RouteKv + "/put"
	RouteKvDelete = transport.RouteKv + "/delete"
	RouteKvTxn    = transport.RouteKv + "/txn"

	RouteLeaseGrant     = transport.RouteLease + "/grant"
	RouteLeaseRevoke    = transport.RouteLease + "/revoke"
	RouteLeaseKeepAlive = transport.RouteLease + "/keep-alive"
	RouteLeaseLookup    = transport.RouteLease + "/lookup"

	RouteOtInit     = transport.RouteOt + "/init"
	RouteOtTransfer = transport.RouteOt + "/transfer"
	RouteOtWriteAll = transport.RouteOt + "/write-all"

	RouteWatchWS = transport.RouteWatch

	RouteJoinCluster       = transport.RouteAdmin + "/join"
	RouteTriggerCompaction = transport.RouteAdmin + "/compaction"
	RouteKillNode          = transport.RouteAdmin + "/kill"

	RouteStats   = "/stats"
	RouteMetrics = "/metrics"
	RouteLivez   = "/livez"
	RouteReadyz  = "/readyz"
)

const (
	kvRangeErrMsg  = "failed to range over key-value pair(s)"
	kvPutErrMsg    = "failed to put key-value pair"
	kvDeleteErrMsg = "failed to delete key-value pair"
	kvTxnErrMsg    = "failed to execute key-value transaction"

	leaseGrantErrMsg     = "failed to grant lease"
	leaseRevokeErrMsg    = "failed to revoke lease"
	leaseKeepAliveErrMsg = "failed to keep lease alive"
	leaseLookupErrMsg    = "failed to lookup lease"

	otInitErrMsg     = "failed to execute OT init"
	otTransferErrMsg = "failed to execute OT transfer"
	otWriteAllErrMsg = "failed to execute OT write all"

	clusterJoinErrMsg = "failed to add peer to the cluster"

	jsonEncodeErrMsg = "failed to encode JSON body"
	jsonDecodeErrMsg = "failed to decode JSON body"

	killNodeErrMsg = "failed to kill node"
)

var (
	errRaftShutdown = errors.New("raft is shutdown")
	// statsErrMsg  string = "stats check failed"
	livezErrMsg  string = "livez check failed"
	readyzErrMsg string = "readzy check failed"
)

type HttpServer struct {
	me              peer.Peer
	kvSvc           service.KVService
	leaseSvc        service.LeaseService
	otSvc           service.OTService
	raftSvc         service.RaftService
	watchHub        *watch.WatchHub
	store           mvcc.StoreMetaReader
	nodeKillSwitch  func()
	readLimiter     *rateLimiter
	writeLimiter    *rateLimiter
	logger          *slog.Logger
	rootLogger      *slog.Logger
	server          *http.Server
	_adminAuthToken string
}

func NewHTTPServer(
	logger *slog.Logger,
	me peer.Peer,
	discoveryService peer.DiscoveryService,
	kvService service.KVService,
	leaseService service.LeaseService,
	otService service.OTService,
	raftService service.RaftService,
	watchHub *watch.WatchHub,
	store mvcc.StoreMetaReader,
	nodeKillSwitch func(),
	reg *prometheus.Registry,
	readLimitConfig ratelimiterConfig,
	writeLimitConfig ratelimiterConfig,
	adminAuthToken string,
) *HttpServer {
	advAddr := me.GetHttpAdvertisedAddress()
	listenAddr := me.GetHttpListenAddress()
	mux := http.NewServeMux()

	s := new(HttpServer)
	s.me = me
	s.kvSvc = kvService
	s.leaseSvc = leaseService
	s.otSvc = otService
	s.raftSvc = raftService
	s.watchHub = watchHub
	s.store = store
	s.logger = logger.With("component", "http_server", "addr", advAddr)
	s.rootLogger = logger
	s.server = &http.Server{
		Addr:    listenAddr,
		Handler: s.panicRecoveryMiddleware(s.corsMuxMiddleware(mux)),
	}
	s._adminAuthToken = adminAuthToken
	s.nodeKillSwitch = nodeKillSwitch

	// TODO(ratelimiter): run real benchmarks to determine rps and burst: something around 75% of peak capacity
	s.readLimiter = newRateLimiter(readLimitConfig)
	s.writeLimiter = newRateLimiter(writeLimitConfig)
	s.logger.Info("Rate limits initialized",
		"read_rps", readLimitConfig.Rps,
		"write_rps", writeLimitConfig.Rps,
		"read_burst", readLimitConfig.Burst,
		"write_burst", writeLimitConfig.Burst,
	)

	// kv
	mux.Handle("POST "+RouteKvRange, s.strongReadChain(s.handleKvRange)) // hack so browsers accept GET with body
	mux.Handle("GET "+RouteKvRange, s.strongReadChain(s.handleKvRange))
	mux.Handle("POST "+RouteKvPut, s.writeChain(s.handleKvPut))
	mux.Handle("DELETE "+RouteKvDelete, s.writeChain(s.handleKvDelete))
	mux.Handle("POST "+RouteKvTxn, s.writeChain(s.handleKvTxn))

	// lease
	mux.Handle("POST "+RouteLeaseGrant, s.writeChain(s.handleLeaseGrant))
	mux.Handle("DELETE "+RouteLeaseRevoke, s.writeChain(s.handleLeaseRevoke))
	mux.Handle("POST "+RouteLeaseKeepAlive, s.writeChain(s.handleLeaseKeepAlive))
	// since leases are time sensitive, the only correct lookup should be done on the leader
	mux.Handle("POST "+RouteLeaseLookup, chain(http.HandlerFunc(s.handleLeaseLookup), s.requestLoggingMiddleware, s.readLimitMiddleware, s.leaderMiddleware)) // hack so browsers accept GET with body
	mux.Handle("GET "+RouteLeaseLookup, chain(http.HandlerFunc(s.handleLeaseLookup), s.requestLoggingMiddleware, s.readLimitMiddleware, s.leaderMiddleware))

	// ot
	mux.Handle("POST "+RouteOtInit, s.weakReadChain(s.handleOTInit)) // hack so browsers accept GET with body
	mux.Handle("GET "+RouteOtInit, s.weakReadChain(s.handleOTInit))
	mux.Handle("POST "+RouteOtTransfer, s.strongReadChain(s.handleOTTransfer)) // hack so browsers accept GET with body
	mux.Handle("GET "+RouteOtTransfer, s.strongReadChain(s.handleOTTransfer))
	mux.Handle("POST "+RouteOtWriteAll, s.writeChain(s.handleOTWriteAll))

	// watch: watches can be served from any node
	mux.Handle("GET "+RouteWatchWS, s.weakReadChain(s.handleWatch))

	// admin / protected routes (auth WIP):
	mux.Handle("POST "+RouteJoinCluster, s.adminChain(s.handleJoin))                                               // cluster
	mux.Handle("DELETE "+RouteKillNode, s.adminChain(s.handleKillNode))                                            // kill a node given its id (forwarding request to given node, then killing self)
	mux.Handle("POST "+RouteTriggerCompaction, chain(s.adminChain(s.handleCompactionTrigger), s.leaderMiddleware)) // manual compaction trigger, must go through leader

	// health / debug
	mux.Handle("GET "+RouteStats, chain(http.HandlerFunc(s.handleStats), s.readLimitMiddleware, s.leaderMiddleware)) // stats
	mux.HandleFunc("GET "+RouteLivez, s.handleLivez)                                                                 // k8s /livez
	mux.HandleFunc("GET "+RouteReadyz, s.handleReadyz)                                                               // k8s /readyz
	mux.Handle("GET "+RouteMetrics, http.TimeoutHandler(
		promhttp.HandlerFor(reg, promhttp.HandlerOpts{}),
		5*time.Second,
		"metrics timeout",
	))

	return s
}

func (s *HttpServer) Start() error {
	s.logger.Info("Starting HTTP server", "addr", s.server.Addr)
	if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *HttpServer) Shutdown(ctx context.Context) error {
	s.logger.Info("Stopping HTTP server")
	s.server.SetKeepAlivesEnabled(false)
	if err := s.server.Shutdown(ctx); err != nil {
		err := s.server.Close()
		s.logger.Error("closing server failed", "error", err)
		return err
	}
	return nil
}

func (s *HttpServer) handleKvRange(w http.ResponseWriter, r *http.Request) {
	var req api.RangeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, jsonDecodeErrMsg, err, http.StatusBadRequest)
		return
	}

	status := http.StatusOK
	response, err := s.kvSvc.Range(r.Context(), req)
	if err != nil {
		s.logger.Error(kvRangeErrMsg, "error", err)
		if errors.Is(err, util.ErrPropose) {
			status = http.StatusServiceUnavailable
		} else {
			status = http.StatusBadRequest
		}
		s.writeError(w, kvRangeErrMsg, err, status)
		return
	}
	s.writeJSON(w, response, status)
}

func (s *HttpServer) handleKvPut(w http.ResponseWriter, r *http.Request) {
	var req api.PutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, jsonDecodeErrMsg, err, http.StatusBadRequest)
		return
	}

	status := http.StatusOK
	response, err := s.kvSvc.Put(r.Context(), req)
	if err != nil {
		if errors.Is(err, util.ErrPropose) {
			status = http.StatusServiceUnavailable
		} else {
			status = http.StatusBadRequest
		}
		s.writeError(w, kvPutErrMsg, err, status)
		return
	}
	s.writeJSON(w, response, status)
}

func (s *HttpServer) handleKvDelete(w http.ResponseWriter, r *http.Request) {
	var req api.DeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, jsonDecodeErrMsg, err, http.StatusBadRequest)
		return
	}

	status := http.StatusOK
	response, err := s.kvSvc.Delete(r.Context(), req)
	if err != nil {
		if errors.Is(err, util.ErrPropose) {
			status = http.StatusServiceUnavailable
		} else {
			status = http.StatusBadRequest
		}
		s.writeError(w, kvDeleteErrMsg, err, status)
		return
	}
	s.writeJSON(w, response, status)
}

func (s *HttpServer) handleKvTxn(w http.ResponseWriter, r *http.Request) {
	var req api.TxnRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, jsonDecodeErrMsg, err, http.StatusBadRequest)
		return
	}

	status := http.StatusOK
	response, err := s.kvSvc.Txn(r.Context(), req)
	if err != nil {
		if errors.Is(err, util.ErrPropose) {
			status = http.StatusServiceUnavailable
		} else {
			status = http.StatusBadRequest
		}
		s.writeError(w, kvTxnErrMsg, err, status)
		return
	}
	s.writeJSON(w, response, status)
}

func (s *HttpServer) handleJoin(w http.ResponseWriter, r *http.Request) {
	var req transport.JoinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, jsonDecodeErrMsg, err, http.StatusBadRequest)
		return
	}

	err := s.raftSvc.AddToCluster(r.Context(), req)
	if err != nil {
		s.writeError(w, clusterJoinErrMsg, err, http.StatusInternalServerError)
		return
	}

	s.logger.Info("Peer added to cluster successfully")
	response := map[string]string{"message": "Peer added to cluster successfully"}
	s.writeJSON(w, response, http.StatusOK)
}

func (s *HttpServer) handleLeaseGrant(w http.ResponseWriter, r *http.Request) {
	var req api.LeaseGrantRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, jsonDecodeErrMsg, err, http.StatusBadRequest)
		return
	}

	status := http.StatusOK
	response, err := s.leaseSvc.Grant(r.Context(), req)
	if err != nil {
		if errors.Is(err, util.ErrPropose) {
			status = http.StatusServiceUnavailable
		} else {
			status = http.StatusBadRequest
		}
		s.writeError(w, leaseGrantErrMsg, err, status)
		return
	}
	s.writeJSON(w, response, status)
}
func (s *HttpServer) handleLeaseRevoke(w http.ResponseWriter, r *http.Request) {
	var req api.LeaseRevokeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, jsonDecodeErrMsg, err, http.StatusBadRequest)
		return
	}

	status := http.StatusOK
	response, err := s.leaseSvc.Revoke(r.Context(), req)
	if err != nil {
		if errors.Is(err, util.ErrPropose) {
			status = http.StatusServiceUnavailable
		} else if errors.Is(err, lease.ErrLeaseNotFound) {
			status = http.StatusNotFound
		} else {
			status = http.StatusBadRequest
		}
		s.writeError(w, leaseRevokeErrMsg, err, status)
		return
	}
	s.writeJSON(w, response, status)
}

func (s *HttpServer) handleLeaseKeepAlive(w http.ResponseWriter, r *http.Request) {
	var req api.LeaseKeepAliveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, jsonDecodeErrMsg, err, http.StatusBadRequest)
		return
	}

	status := http.StatusOK
	response, err := s.leaseSvc.KeepAlive(r.Context(), req)
	if err != nil {
		if errors.Is(err, util.ErrPropose) {
			status = http.StatusServiceUnavailable
		} else if errors.Is(err, lease.ErrLeaseNotFound) {
			status = http.StatusNotFound
		} else {
			status = http.StatusBadRequest
		}
		s.writeError(w, leaseKeepAliveErrMsg, err, status)
		return
	}
	s.writeJSON(w, response, status)
}

func (s *HttpServer) handleLeaseLookup(w http.ResponseWriter, r *http.Request) {
	var req api.LeaseLookupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, jsonDecodeErrMsg, err, http.StatusBadRequest)
		return
	}

	status := http.StatusOK
	response, err := s.leaseSvc.Lookup(r.Context(), req)
	if err != nil {
		if errors.Is(err, util.ErrPropose) {
			status = http.StatusServiceUnavailable
		} else if errors.Is(err, lease.ErrLeaseNotFound) {
			status = http.StatusNotFound
		} else {
			status = http.StatusBadRequest
		}
		s.writeError(w, leaseLookupErrMsg, err, status)
		return
	}
	s.writeJSON(w, response, status)
}

func (s *HttpServer) handleOTInit(w http.ResponseWriter, r *http.Request) {
	// req is empty by desing as of yet, still
	// an empty body is the valid one,
	// we dont want to get sent random json
	var req api.OTInitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, jsonDecodeErrMsg, err, http.StatusBadRequest)
		return
	}

	status := http.StatusOK
	response, err := s.otSvc.Init(r.Context(), req)
	if err != nil {
		if errors.Is(err, util.ErrPropose) {
			status = http.StatusServiceUnavailable
		} else {
			status = http.StatusBadRequest
		}
		s.writeError(w, otInitErrMsg, err, status)
		return
	}
	s.writeJSON(w, response, status)
}

func (s *HttpServer) handleOTTransfer(w http.ResponseWriter, r *http.Request) {
	var req api.OTTransferRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, jsonDecodeErrMsg, err, http.StatusBadRequest)
		return
	}

	status := http.StatusOK
	reponse, err := s.otSvc.Transfer(r.Context(), req)
	if err != nil {
		if errors.Is(err, util.ErrPropose) {
			status = http.StatusServiceUnavailable
		} else {
			status = http.StatusBadRequest
		}
		s.writeError(w, otTransferErrMsg, err, status)
		return
	}
	s.writeJSON(w, reponse, status)
}

func (s *HttpServer) handleOTWriteAll(w http.ResponseWriter, r *http.Request) {
	var req api.OTWriteAllRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, jsonDecodeErrMsg, err, http.StatusBadRequest)
		return
	}

	status := http.StatusOK
	response, err := s.otSvc.WriteAll(r.Context(), req)
	if err != nil {
		if errors.Is(err, util.ErrPropose) {
			status = http.StatusServiceUnavailable
		} else {
			status = http.StatusBadRequest
		}
		s.writeError(w, otWriteAllErrMsg, err, status)
		return
	}
	s.writeJSON(w, response, status)
}

func (s *HttpServer) handleStats(w http.ResponseWriter, r *http.Request) {
	stats := s.raftSvc.Stats(r.Context())
	s.writeJSON(w, stats, http.StatusOK)
}

func (s *HttpServer) handleLivez(w http.ResponseWriter, r *http.Request) {
	if s.raftSvc.RaftState() == raft.Shutdown {
		s.writeError(w, livezErrMsg, errRaftShutdown, http.StatusServiceUnavailable)
		return
	}

	if err := s.kvSvc.Ping(); err != nil {
		s.writeError(w, "kv store ping failed", err, http.StatusServiceUnavailable)
		return
	}

	s.writeJSON(w, map[string]string{"status": "ok"}, http.StatusOK)
}

func (s *HttpServer) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if s.raftSvc.RaftState() == raft.Shutdown {
		s.writeError(w, readyzErrMsg, errRaftShutdown, http.StatusServiceUnavailable)
		return
	}

	_, err := s.raftSvc.Leader(r.Context())
	if err != nil {
		msg := readyzErrMsg + ": failed to get leader info"
		s.writeError(w, msg, err, http.StatusServiceUnavailable)
		return
	}

	if err := s.raftSvc.LaggingBehind(); err != nil {
		msg := readyzErrMsg + ": raft is lagging behind"
		s.writeError(w, msg, err, http.StatusServiceUnavailable)
		return
	}

	s.writeJSON(w, map[string]string{"status": "ok"}, http.StatusOK)
}

func (s *HttpServer) handleWatch(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols:   []string{watch.WatchSubprotocol},
		OriginPatterns: []string{"*"}, // for the frontend, might be risky
	})

	if err != nil {
		msg := "failed to accept connection"
		s.writeError(w, msg, err, http.StatusBadRequest)
		return
	}

	defer func() {
		_ = conn.CloseNow()
	}()

	if conn.Subprotocol() != watch.WatchSubprotocol {
		msg := fmt.Sprintf("client must speak '%s'", watch.WatchSubprotocol)
		s.logger.Error("faild to accept connection", "error", msg)
		_ = conn.Close(websocket.StatusPolicyViolation, msg)
		return
	}

	session := watch.NewSession(conn, r.Context(), s.watchHub, s.store, s.me, s.rootLogger, 0, 0)
	defer session.Close()

	if err = session.Run(); err != nil {
		msg := "watch handler failed, closing connection"
		s.logger.Error(msg, "error", err)
		_ = conn.Close(websocket.StatusAbnormalClosure, fmt.Sprintf("%s: %v", msg, err))
		return
	}
	s.logger.Info("watch handler done, closing connection")
	if err = conn.Close(websocket.StatusNormalClosure, "all good"); err != nil {
		s.writeError(w, "failed to close websocket", err, http.StatusInternalServerError)
	}
}

func (s *HttpServer) handleCompactionTrigger(w http.ResponseWriter, r *http.Request) {
	var req api.CompactionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, jsonDecodeErrMsg, err, http.StatusBadRequest)
		return
	}

	status := http.StatusOK
	response, err := s.kvSvc.TriggerCompaction(r.Context(), req)
	if err != nil {
		if errors.Is(err, util.ErrPropose) {
			status = http.StatusServiceUnavailable
		} else {
			status = http.StatusBadRequest
		}
		s.writeError(w, "compaction trigger failed", err, status)
		return
	}
	s.writeJSON(w, response, status)
}

func (s *HttpServer) handleKillNode(w http.ResponseWriter, r *http.Request) {
	var req api.KillNodeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, jsonDecodeErrMsg, err, http.StatusBadRequest)
		return
	}

	victimID := req.NodeID

	if len(victimID) == 0 {
		s.writeError(w, killNodeErrMsg, errors.New("empty node id"), http.StatusBadRequest)
		return
	}

	if s.me.NodeID == victimID {
		s.nodeKillSwitch()
		s.writeJSON(w, api.KillNodeRespone{Success: true}, http.StatusOK)
		return
	}

	for _, p := range s.raftSvc.Peers() {
		if p.NodeID == victimID {
			s.logger.Info("killing node: found victim peer, proxying", "victim_id", victimID)
			proxy := newReverseProxy(p.GetHttpAdvertisedAddress())
			proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
				if networkerr(err) {
					s.writeError(w, killNodeErrMsg+" (proxied)", err, http.StatusServiceUnavailable)
					return
				}
				s.writeError(w, killNodeErrMsg+" (proxied)", err, http.StatusInternalServerError)
			}
			proxy.ServeHTTP(w, r)
			return
		}
	}

	s.writeError(w, killNodeErrMsg, fmt.Errorf("node not found with id %s", victimID), http.StatusNotFound)
}

func (s *HttpServer) writeJSON(w http.ResponseWriter, response any, status int) {
	w.Header().Set("Content-Type", "application/json")
	bytes, err := json.Marshal(response)
	if err != nil {
		s.logger.Error("Failed to marshal JSON response", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		if _, err = w.Write([]byte(jsonEncodeErrMsg + ": " + err.Error())); err != nil {
			s.logger.Error("Failed to write JSON response", "error", err)
		}
		return
	}

	w.WriteHeader(status)
	if _, err := w.Write(bytes); err != nil {
		s.logger.Error("Failed to write JSON response", "error", err)
	}
}

// writeError logs the message with the provided error then composes them
// into a single string in the form "msg: error" and writes it as a JSON response
// with the provided status code. The optional attrs will be provided to the logger as additional context
func (s *HttpServer) writeError(w http.ResponseWriter, msg string, err error, status int, attrs ...any) {
	attrs = append(attrs, "error", err.Error())
	s.logger.Error(msg, attrs...)
	jsonMessage := fmt.Sprintf("%s: %v", msg, err)
	w.Header().Set("Content-Type", "application/json")
	bytes, _ := json.Marshal(map[string]string{"error": jsonMessage})
	w.WriteHeader(status)
	if _, err := w.Write(bytes); err != nil {
		s.logger.Error("Failed to write error response", "error", err)
	}
}

// readLimitMiddleware is a wrapper around the internal read rate limiters in the [HttpServer]
// that fits the type definition of [middleware] functions.
func (s *HttpServer) readLimitMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		l := s.readLimiter.Limiter(r.URL.Path)

		if !l.Allow() {
			s.writeError(w, "read limit exceeded", errMsgRateLimiter, http.StatusTooManyRequests)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// writeMiddleware is a wrapper around the internal write rate limiters in the [HttpServer]
// that fits the type definition of [middleware] functions.
func (s *HttpServer) writeLimitMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		l := s.writeLimiter.Limiter(r.URL.Path)

		if !l.Allow() {
			s.writeError(w, "write limit exceeded", errMsgRateLimiter, http.StatusTooManyRequests)
			return
		}

		next.ServeHTTP(w, r)
	}))
}
