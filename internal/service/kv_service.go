package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/fsm"
	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/mvcc"
	"github.com/balits/kave/internal/peer"
	"github.com/balits/kave/internal/types/api"
	"github.com/balits/kave/internal/util"
)

var ErrManualCompaction = errors.New("failed to compact store manually")

type KVService interface {
	Range(ctx context.Context, req api.RangeRequest) (*api.RangeResponse, error)
	Put(ctx context.Context, req api.PutRequest) (*api.PutResponse, error)
	Delete(ctx context.Context, req api.DeleteRequest) (*api.DeleteResponse, error)
	Txn(ctx context.Context, req api.TxnRequest) (*api.TxnResponse, error)
	TriggerCompaction(ctx context.Context, req api.CompactionRequest) (*api.CompactionResponse, error)

	Ping() error
}

type kvSvc struct {
	me      peer.Peer
	opts    kv.Options
	store   mvcc.ReadOnlyStore
	propose util.ProposeFunc
	raftSvc RaftService
	logger  *slog.Logger
}

func NewKVService(logger *slog.Logger, me peer.Peer, store mvcc.ReadOnlyStore, peerSvc RaftService, opts kv.Options, proposeFunc util.ProposeFunc) KVService {
	return &kvSvc{
		me:      me,
		opts:    opts,
		store:   store,
		propose: proposeFunc,
		raftSvc: peerSvc,
		logger:  logger.With("component", "kv_service"),
	}
}

func (s *kvSvc) Range(ctx context.Context, req api.RangeRequest) (*api.RangeResponse, error) {
	s.logger.WithGroup("request").
		Info("Range request received",
			"key", req.Key,
			"end", req.End,
			"limit", req.Limit,
			"revision", req.Revision,
			"prefix", req.Prefix,
			"countOnly", req.CountOnly,
			"serializable", req.Serializable,
		)

	if err := req.Check(); err != nil {
		return nil, fmt.Errorf("range failed: malformed request: %w", err)
	}

	if !req.Serializable {
		if err := s.raftSvc.VerifyLeader(ctx); err != nil {
			return nil, fmt.Errorf("range failed: failed to verify leader: %v", err)
		}
	}

	if req.Prefix {
		req.End = kv.PrefixEnd(req.Key)
	}

	r := s.store.NewReader()
	entries, count, _, err := r.Range(req.Key, req.End, req.Revision, req.Limit)
	if err != nil {
		return nil, fmt.Errorf("range failed: %v", err)
	}

	res := new(api.RangeResponse)
	res.Count = count
	if !req.CountOnly {
		res.Entries = entries
	}

	currRev, compactedRev, raftIndex, raftTerm := s.store.Meta()
	res.Header = api.ResponseHeader{
		Revision:     currRev.Main,
		CompactedRev: compactedRev,
		RaftTerm:     raftTerm,
		RaftIndex:    raftIndex,
		NodeID:       s.me.NodeID,
	}

	s.logger.WithGroup("response").
		Debug("Range request succeeded",
			"count", res.Count,
			"entries", res.Entries,
		)

	return res, nil
}

func (s *kvSvc) Put(ctx context.Context, req api.PutRequest) (*api.PutResponse, error) {
	// TODO: how to debug key, value if they can be kilobytes big?
	s.logger.WithGroup("request").
		Info("Put request received",
			"key", req.Key,
			"value", req.Value,
			"prevEntry", req.PrevEntry,
			"leaseID", req.LeaseID,
			"ignoreValue", req.IgnoreValue,
			"renewLease", req.IgnoreLease,
		)

	if err := req.Check(); err != nil {
		return nil, fmt.Errorf("put failed: malformed request: %w", err)
	}

	if err := s.opts.CheckKey(req.Key); err != nil {
		return nil, fmt.Errorf("put failed: malformed request: %w", err)
	}

	if err := s.opts.CheckValue(req.Value); err != nil {
		return nil, fmt.Errorf("put failed: malformed request: %w", err)
	}

	cmd := command.Command{
		Kind: command.KindPut,
		Put:  &req,
	}

	result, err := s.propose(ctx, cmd)
	if err != nil {
		return nil, fmt.Errorf("put failed: %w", err)
	}
	if result.Error != nil {
		return nil, fmt.Errorf("put failed: %w", result.Error)
	}
	if result.Put == nil {
		return nil, fmt.Errorf("put failed: %w", fsm.ErrNilApplyResult)
	}

	return &api.PutResponse{
		Header:              result.Header,
		PutResponseNoHeader: *result.Put,
	}, nil
}

func (s *kvSvc) Delete(ctx context.Context, req api.DeleteRequest) (*api.DeleteResponse, error) {
	s.logger.WithGroup("request").
		Info("Delete request received",
			"key", req.Key,
			"end", req.End,
			"prevEntries", req.PrevEntries,
		)

	if err := req.Check(); err != nil {
		return nil, fmt.Errorf("delete failed: malformed request: %w", err)
	}

	cmd := command.Command{
		Kind:   command.KindDelete,
		Delete: &req,
	}

	result, err := s.propose(ctx, cmd)
	if err != nil {
		return nil, fmt.Errorf("delete failed: %w", err)
	}
	if result.Error != nil {
		return nil, fmt.Errorf("delete failed: %w", result.Error)
	}
	if result.Delete == nil {
		return nil, fmt.Errorf("delete failed: %w", fsm.ErrNilApplyResult)
	}

	return &api.DeleteResponse{
		Header:                 result.Header,
		DeleteResponseNoHeader: *result.Delete,
	}, nil
}
func (s *kvSvc) Txn(ctx context.Context, req api.TxnRequest) (*api.TxnResponse, error) {
	s.logger.WithGroup("request").
		Info("Transaction request received",
			"comparison_len", len(req.Comparisons),
			"success_ops_len", len(req.Success),
			"failure_ops_len", len(req.Failure),
		)

	if err := req.Check(); err != nil {
		return nil, fmt.Errorf("txn failed: malformed request: %w", err)
	}

	cmd := command.Command{
		Kind: command.KindTxn,
		Txn:  &req,
	}

	result, err := s.propose(ctx, cmd)
	if err != nil {
		return nil, fmt.Errorf("txn failed: %w", err)
	}
	if result.Error != nil {
		return nil, fmt.Errorf("txn failed: %w", result.Error)
	}
	if result.Txn == nil {
		return nil, fmt.Errorf("txn failed: %w", fsm.ErrNilApplyResult)
	}

	return &api.TxnResponse{
		Header:            result.Header,
		TxnResultNoHeader: *result.Txn,
	}, nil
}

func (s *kvSvc) Ping() error {
	return s.store.Ping()
}

func (s *kvSvc) TriggerCompaction(ctx context.Context, req api.CompactionRequest) (*api.CompactionResponse, error) {
	l := s.logger.With(
		"compaction_target_rev", req.TargetRev,
	)

	cmd := command.Command{
		Kind:       command.KindCompaction,
		Compaction: &req,
	}

	result, err := s.propose(ctx, cmd)
	if err != nil {
		l.Warn("failed to compact manually: failed to propose compaction", "error", err)
		return nil, fmt.Errorf("%w: %s", ErrManualCompaction, err)
	}

	if result.Error != nil {
		l.Warn("failed to compact manually", "error", result.Error)
		return nil, fmt.Errorf("%w: %s", ErrManualCompaction, result.Error)
	}

	if result.Compaction == nil {
		l.Error("failed to compact manually", "error", "compaction result was nil")
		return nil, fmt.Errorf("%w: compaction result was nil", ErrManualCompaction)
	}

	if result.Compaction.Error != nil {
		l.Warn("failed to compact manually", "error", result.Compaction.Error)
		return nil, fmt.Errorf("%w: %s", ErrManualCompaction, result.Compaction.Error)
	}

	select {
	case <-result.Compaction.DoneC:
		l.Info("compaction finished successfuly")

		currentRev, compactedRev, raftIndex, raftTerm := s.store.Meta()
		header := api.ResponseHeader{
			Revision:     currentRev.Main,
			CompactedRev: compactedRev,
			RaftTerm:     raftTerm,
			RaftIndex:    raftIndex,
			NodeID:       s.me.NodeID,
		}

		return &api.CompactionResponse{
			Header:                     header,
			CompactionResponseNoHeader: api.CompactionResponseNoHeader{Success: true},
		}, nil
	case <-ctx.Done():
		l.Error("failed to compact manually", "error", "context cancelled while waiting for compaction to finish")
		return nil, fmt.Errorf("%w: %s", ErrManualCompaction, ctx.Err())
	}
}
