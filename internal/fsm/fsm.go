package fsm

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/lease"
	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/mvcc"
	"github.com/balits/kave/internal/ot"
	"github.com/balits/kave/internal/peer"
	"github.com/balits/kave/internal/storage/backend"
	"github.com/hashicorp/raft"
)

var (
	ErrStateMachineError = errors.New("FSM error")
	ErrNilApplyResult    = fmt.Errorf("%w: nil result from FSM", ErrStateMachineError)
)

type ApplyContext struct {
	Index  uint64
	Term   uint64
	NodeID string
}

type Fsm struct {
	me             peer.Peer
	backend        backend.Backend
	store          *mvcc.KvStore
	engine         *mvcc.Engine
	lm             *lease.LeaseManager
	om             *ot.OTManager
	metrics        *metrics.RaftMetrics
	writeObservers []WriteObserver
	logger         *slog.Logger
}

func NewWithEngine(logger *slog.Logger, me peer.Peer, b backend.Backend, store *mvcc.KvStore, lm *lease.LeaseManager, om *ot.OTManager, engine *mvcc.Engine) *Fsm {
	f := &Fsm{
		me:      me,
		backend: b,
		store:   store,
		engine:  engine,
		lm:      lm,
		om:      om,
		logger:  logger.With("component", "fsm"),
	}
	return f
}

func New(logger *slog.Logger, me peer.Peer, b backend.Backend, store *mvcc.KvStore, lm *lease.LeaseManager, om *ot.OTManager) *Fsm {
	return NewWithEngine(logger, me, b, store, lm, om, mvcc.NewEngine(store, lm))
}

// SetMetrics is needed for a two phase init of the fsm
// because of the unavoidable circular dependency:
//
// 1) raft needs fsm
//
// 2) fsm.metrics need raft
func (f *Fsm) SetMetrics(m *metrics.RaftMetrics) {
	f.metrics = m
}

func (f *Fsm) RegisterObservers(obs ...WriteObserver) {
	f.writeObservers = append(f.writeObservers, obs...)
}

func (f *Fsm) Apply(log *raft.Log) any {
	// this makes test way easier
	if f.metrics != nil {
		start := time.Now()
		defer func() { f.metrics.ApplyDurationSec.Observe(time.Since(start).Seconds()) }()
		f.metrics.ApplyTotal.Inc()
	}

	cmd, err := command.Decode(log.Data)
	if err != nil {
		return command.Result{Error: err}
	}

	if err := cmd.Check(); err != nil {
		return command.Result{Error: err}
	}

	lastAppliedIndex, _ := f.store.RaftMeta()
	if log.Index <= lastAppliedIndex {
		f.logger.Debug("skipping already applied raft log (replay)", "log_index", log.Index, "last_applied", lastAppliedIndex)
		return command.Result{}
	}

	f.store.UpdateInmemRaftMeta(log.Index, log.Term)

	res := f.applySingle(ApplyContext{
		Index:  log.Index,
		Term:   log.Term,
		NodeID: f.me.NodeID,
	}, cmd)

	if res.Error == nil {
		for _, o := range f.writeObservers {
			o.OnWrite(res.Header.Revision)
		}
	}

	return res
}

// applySingle applies a decoded, validated command.
// It dispatches to the domain handlers and assigns header fields.
//
// It does NOT perform replay detection, update raft meta, persist state,
// or notify observers — those are orchestration concerns handled by the caller
// (Apply or, in the future, ApplyBatch).
//
// NOTE: The caller must ensure the log entry has not already been applied
// and that UpdateInmemRaftMeta has been called beforehand so that
// domain handlers see the correct raft metadata.
func (f *Fsm) applySingle(ctx ApplyContext, cmd command.Command) command.Result {
	var res command.Result

	switch cmd.Kind {
	case command.KindPut, command.KindDelete, command.KindTxn:
		res = f.applyKv(cmd)
	case command.KindLeaseGrant, command.KindLeaseRevoke, command.KindLeaseKeepAlive,
		command.KindLeaseLookup, command.KindLeaseCheckpoint, command.KindLeaseExpire:
		res = f.applyLease(cmd)
	case command.KindCompaction:
		res = f.applyCompaction(cmd)
	case command.KindOTWriteAll, command.KindOTGenerateClusterKey:
		res = f.applyOT(cmd)
	case "":
		panic("no command kind specified")
	default:
		panic(fmt.Sprintf("unsupported command kind: %v", cmd.Kind))
	}

	res.Header.RaftTerm = ctx.Term
	res.Header.RaftIndex = ctx.Index
	res.Header.NodeID = ctx.NodeID

	return res
}

// TODO: for ApplyBatch, the mvcc.Engine and other managers would need to accept
// an external mvcc.Writer so that all entries in a batch share a single transaction.
// Currently applyKv creates and commits its own writer internally.
func (f *Fsm) applyKv(cmd command.Command) command.Result {
	switch cmd.Kind {
	case command.KindPut, command.KindDelete, command.KindTxn:
	default:
		panic(fmt.Sprintf("applyKv called with non-kv command: %s", cmd.Kind))
	}

	res, err := f.engine.ApplyWrite(cmd)
	if err != nil {
		return command.Result{Error: err}
	}
	return *res
}

func (f *Fsm) applyLease(cmd command.Command) command.Result {
	var res command.Result
	var err error

	switch cmd.Kind {
	case command.KindLeaseGrant:
		res, err = f.applyLeaseGrant(cmd)
	case command.KindLeaseRevoke:
		res, err = f.applyLeaseRevoke(cmd)
	case command.KindLeaseKeepAlive:
		res, err = f.applyLeaseKeepAlive(cmd)
	case command.KindLeaseLookup:
		res, err = f.applyLeaseLookup(cmd)
	case command.KindLeaseCheckpoint:
		err = f.applyLeaseCheckpoint(cmd)
	case command.KindLeaseExpire:
		res, err = f.applyLeaseExpire(cmd)
	default:
		panic(fmt.Sprintf("unsupported lease command type: %v", cmd.Kind))
	}

	if err != nil {
		return command.Result{Error: err}
	}
	return res
}

func (f *Fsm) applyLeaseGrant(cmd command.Command) (command.Result, error) {
	l, err := f.lm.Grant(cmd.LeaseGrant.LeaseID, cmd.LeaseGrant.TTL)
	if err != nil {
		return command.Result{}, err
	}
	return command.Result{
		LeaseGrant: &command.ResultLeaseGrant{
			TTL:     l.TTL,
			LeaseID: l.ID,
		},
	}, nil
}

func (f *Fsm) applyLeaseRevoke(cmd command.Command) (command.Result, error) {
	found, revoked, err := f.lm.Revoke(cmd.LeaseRevoke.LeaseID)
	if err != nil {
		return command.Result{}, err
	}
	return command.Result{
		LeaseRevoke: &command.ResultLeaseRevoke{
			Found:   found,
			Revoked: revoked,
		},
	}, nil
}

func (f *Fsm) applyLeaseKeepAlive(cmd command.Command) (command.Result, error) {
	ttl, err := f.lm.KeepAlive(cmd.LeaseKeepAlive.LeaseID)
	if err != nil {
		return command.Result{}, err
	}
	return command.Result{
		LeaseKeepAlive: &command.ResultLeaseKeepAlive{
			TTL:     ttl,
			LeaseID: cmd.LeaseKeepAlive.LeaseID,
		},
	}, nil
}

func (f *Fsm) applyLeaseLookup(cmd command.Command) (command.Result, error) {
	l, err := f.lm.Lookup(cmd.LeaseLookup.LeaseID)
	if err != nil {
		return command.Result{}, err
	}
	return command.Result{
		LeaseLookup: &command.ResultLeaseLookup{
			LeaseID:      l.ID,
			OriginalTTL:  l.TTL,
			RemainingTTL: l.RemainingTTL(),
		},
	}, nil
}

func (f *Fsm) applyLeaseCheckpoint(cmd command.Command) error {
	f.lm.ApplyCheckpoint(*cmd.LeaseCheckpoint)
	return nil
}

func (f *Fsm) applyLeaseExpire(cmd command.Command) (command.Result, error) {
	subres, err := f.lm.ApplyExpired(*cmd.LeaseExpired)
	res := command.Result{
		LeaseExpire: subres,
	}
	return res, err
}

func (f *Fsm) applyCompaction(cmd command.Command) command.Result {
	if cmd.Kind != command.KindCompaction {
		panic(fmt.Sprintf("applyCompaction called with non-compaction command: %s", cmd.Kind))
	}

	doneC, err := f.store.Compact(cmd.Compaction.TargetRev)
	if err != nil {
		return command.Result{Error: err}
	}
	return command.Result{
		Compaction: &command.CompactionResult{
			DoneC: doneC,
		},
	}
}

func (f *Fsm) applyOT(cmd command.Command) command.Result {
	switch cmd.Kind {
	case command.KindOTGenerateClusterKey:
		if err := f.om.ApplyGenerateClusterKey(cmd.OTGenerateClusterKey.Key); err != nil {
			return command.Result{Error: err}
		}
		return command.Result{}

	case command.KindOTWriteAll:
		sub, err := f.om.ApplyWriteAll(*cmd.OTWriteAll)
		if err != nil {
			return command.Result{Error: err}
		}
		return command.Result{
			OtWriteAll: sub,
		}

	default:
		panic(fmt.Sprintf("unsupported OT command type: %v", cmd.Kind))
	}
}

// Snapshot also should be fast, just take a pointer to the data
func (f *Fsm) Snapshot() (raft.FSMSnapshot, error) {
	return f.store.Snapshot(), nil
}

// Restore can be slower, it will never run concurrently with Apply.
// Also, no metrics should be replayed during restoration!
func (f *Fsm) Restore(snapshot io.ReadCloser) error {
	if err := f.backend.Close(); err != nil {
		f.logger.Error("fsm restore error: failed to close backend", "error", err)
		return err
	}
	if err := f.backend.Restore(snapshot); err != nil {
		f.logger.Error("fsm restore error: failed to restore backend", "error", err)
		return err
	}

	if err := f.store.Restore(); err != nil {
		f.logger.Error("fsm restore error: failed to restore mvcc.KvStore", "error", err)
		return err
	}
	if err := f.lm.Restore(); err != nil {
		f.logger.Error("fsm restore error: failed to restore lease.LeaseManager", "error", err)
		return err
	}
	if err := f.om.Restore(); err != nil {
		f.logger.Error("fsm restore error: failed to restore ot.OTManager", "error", err)
		return err
	}
	return nil
}
