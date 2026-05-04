package mvcc

import (
	"fmt"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"

	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/schema"
	"github.com/balits/kave/internal/storage/backend"
	"github.com/balits/kave/internal/util"
	"github.com/prometheus/client_golang/prometheus"
)

type StoreMetaReader interface {
	Meta() (currentRev kv.Revision, compactedRev int64, logIndex, logTerm uint64)
	Revisions() (currentRev kv.Revision, compactedRev int64)
	KeyCount() int64
}

type ReadOnlyStore interface {
	StoreMetaReader
	Ping() error
	NewReader() Reader
}

const MAX_COMPACTION_BATCH_SIZE int = 100

type KvStore struct {
	storeMu sync.RWMutex // mutex for the whole store, not for backend transactions, used for raft meta and compaction

	backend backend.Backend // storage backend
	kvIndex kv.Index        // key chache

	metaMu           sync.RWMutex // mutex for store metadata, so the next 4 fields
	currentRev       kv.Revision  // up to date revision, updated by writers, read by readers, protected by metaMu
	compactedMainRev int64        // main revision up to which the store has been compacted, protected by metaMu
	raftTerm         uint64       // latest applied raft term, protected by metaMu
	applyIndex       uint64       // latest applied raft index, protected by metaMu

	logger   *slog.Logger
	metrics  *metrics.KVMetrics
	keyCount atomic.Int64 // tracked at MVCC level in writer; exposed as GaugeFunc via metrics
}

func NewKvStoreWithIndex(reg prometheus.Registerer, logger *slog.Logger, b backend.Backend, index kv.Index) *KvStore {
	s := &KvStore{
		backend: b,
		kvIndex: index,
		logger:  logger.With("component", "kvstore"),
	}
	s.metrics = newKVMetrics(reg, s)

	return s
}

func NewKvStore(reg prometheus.Registerer, logger *slog.Logger, b backend.Backend) *KvStore {
	return NewKvStoreWithIndex(reg, logger, b, kv.NewTreeIndex(logger))
}

func (s *KvStore) Revisions() (currentRev kv.Revision, compacted int64) {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	return s.currentRev, s.compactedMainRev
}

func (s *KvStore) Meta() (currentRev kv.Revision, compactedRev int64, logIndex, logTerm uint64) {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	return s.currentRev, s.compactedMainRev, s.applyIndex, s.raftTerm
}

func (s *KvStore) KeyCount() int64 {
	return s.keyCount.Load()
}

// Writer acquires RLock so multiple writers can proceed concurrently
// (they're serialized by raft anyway, so only one writer exists at a time,
// but the RLock is correct because writers don't mutate KVStore's structure)
//
// Locks get releaseed in Writer.End()
//
// Locking the store rwlock blocks concurrent writes and restores
func (s *KvStore) NewWriter() Writer {
	// locks gets released in writer.End()
	s.storeMu.RLock()

	s.metaMu.Lock()
	currentRev := s.currentRev
	s.metaMu.Unlock()

	wtx := s.backend.WriteTx()
	wtx.Lock()
	w := newWriter(s, wtx, currentRev)
	return w
}

func (s *KvStore) NewReader() Reader {
	return &reader{store: s, metrics: s.metrics}
}

func (s *KvStore) UpdateInmemRaftMeta(logIndex, term uint64) {
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	s.applyIndex = logIndex
	s.raftTerm = term
}

func (s *KvStore) PersistRaftMeta() {
	s.metaMu.RLock()
	idx := s.applyIndex
	term := s.raftTerm
	s.metaMu.RUnlock()

	wtx := s.backend.WriteTx()
	wtx.Lock()
	defer wtx.Unlock()
	_ = wtx.UnsafePut(schema.BucketMeta, schema.KeyRaftApplyIndex, util.EncodeUint64(idx))
	_ = wtx.UnsafePut(schema.BucketMeta, schema.KeyRaftTerm, util.EncodeUint64(term))
	_, _ = wtx.Commit()
}

func (s *KvStore) RaftMeta() (logIndex, term uint64) {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	return s.applyIndex, s.raftTerm
}

func (s *KvStore) Restore() error {
	s.storeMu.Lock()
	defer s.storeMu.Unlock()

	restoredKey := int64(0)
	lastRev := kv.Revision{Main: 0, Sub: 0}
	min := kv.EncodeRevisionAsBucketKey(lastRev, kv.NewRevBytes())
	max := kv.EncodeRevisionAsBucketKey(kv.Revision{Main: math.MaxInt64, Sub: math.MaxInt64}, kv.NewRevBytes())

	s.kvIndex.Clear()
	rtx := s.backend.ReadTx()
	rtx.RLock()
	err := rtx.UnsafeScan(schema.BucketKV, min, max, func(k, v []byte) error {
		bk := kv.DecodeKvBucketKey(k)
		entry, err := kv.DecodeEntry(v)
		if bk.Tombstone {
			if err := s.kvIndex.Tombstone(entry.Key, bk.Revision); err != nil {
				s.logger.Warn("restore error: failed to tombstone entry in the kvIndex", "error", err)
			}
		} else {
			if err != nil {
				s.logger.Warn("restore error: failed to decode entry", "error", err)
			}
			if err := s.kvIndex.Put(entry.Key, bk.Revision); err != nil {
				s.logger.Warn("restore error: failed to put entry into kvIndex", "error", err)
			}
		}
		lastRev = bk.Revision
		restoredKey++
		return nil
	})
	if err != nil {
		s.logger.Error("restore error: scanning over keys errored", "error", err)
	}

	raftTermBytes, err := rtx.UnsafeGet(schema.BucketMeta, schema.KeyRaftTerm)
	if err != nil {
		s.logger.Error("restore error: failed to get raft term", "error", err)
	}
	raftIndexBytes, err := rtx.UnsafeGet(schema.BucketMeta, schema.KeyRaftApplyIndex)
	if err != nil {
		s.logger.Error("restore error: failed to get raft index", "error", err)
	}
	finishedCompactedRevBytes, err := rtx.UnsafeGet(schema.BucketMeta, schema.KeyFinishedCompactedRev)
	if err != nil {
		s.logger.Error("restore error: failed to get finished compacted rev", "error", err)
	}
	scheduledCompactedRevBytes, err := rtx.UnsafeGet(schema.BucketMeta, schema.KeyScheduledCompactedRev)
	if err != nil {
		s.logger.Error("restore error: failed to get schedueld compacted rev", "error", err)
	}
	rtx.RUnlock()

	term, err := util.DecodeUint64(raftTermBytes)
	if err != nil {
		s.logger.Error("restore error: failed to decode raft term", "error", err)
	}
	raftIndex, err := util.DecodeUint64(raftIndexBytes)
	if err != nil {
		s.logger.Error("restore error: failed to decode raft index", "error", err)
	}
	finishedCompactedRev, err := util.DecodeInt64(finishedCompactedRevBytes)
	if err != nil {
		s.logger.Error("restore error: failed to decode finished compacted rev", "error", err)
	}
	scheduledCompactedRev, err := util.DecodeInt64(scheduledCompactedRevBytes)
	if err != nil {
		s.logger.Error("restore error: failed to decode scheduled compacted rev", "error", err)
	}

	var finalCompacteRev int64
	if scheduledCompactedRev > finishedCompactedRev {
		finalCompacteRev = scheduledCompactedRev
		s.logger.Info("resuming interrupted compaction from previous run",
			"target_rev", scheduledCompactedRev,
			"previously_finished_rev", finishedCompactedRev,
		)
		go s.doCompact(scheduledCompactedRev, finishedCompactedRev)
	} else {
		finalCompacteRev = finishedCompactedRev
	}

	s.metaMu.Lock()
	s.raftTerm = term
	s.applyIndex = raftIndex
	s.currentRev = lastRev
	s.compactedMainRev = finalCompacteRev
	s.metaMu.Unlock()

	s.keyCount.Store(restoredKey)

	return nil
}

func (s *KvStore) Snapshot() Snapshot {
	return Snapshot{s}
}

// Compact compacts the store up to the given revision.
// All superseded key revisions with main revision < rev will be removed.
// Compaction happens in two phases, the last happends concurrently
// 1) Persist schedule compaction revision -> crash safe
// 2) Execute comapction + Update finished compaction revision
func (s *KvStore) Compact(rev int64) (<-chan struct{}, error) {
	s.metaMu.Lock()
	defer s.metaMu.Unlock() // lock rev for the whole compaction, so that no other gorutine could schedule a compaction

	if rev < 0 {
		return nil, fmt.Errorf("compaction error: compaction target revision cannot be negative")
	} else if rev > s.currentRev.Main {
		return nil, fmt.Errorf("compaction error: compaction target revision cannot be higher than current revision")
	} else if rev <= s.compactedMainRev {
		return nil, fmt.Errorf("compaction error: %v", kv.ErrCompacted)
	}
	// Persist schedule compaction revision -> crash safe
	{
		wtx := s.backend.WriteTx()
		wtx.Lock()
		revBytes := kv.EncodeRevisionAsBucketKey(kv.Revision{Main: rev}, kv.NewRevBytes())
		if err := wtx.UnsafePut(schema.BucketMeta, schema.KeyScheduledCompactedRev, revBytes); err != nil {
			wtx.Unlock()
			s.logger.Warn("compaction error: failed to persist compaction revisions", "error", err)
			return nil, err
		}
		if _, err := wtx.Commit(); err != nil {
			wtx.Rollback()
			wtx.Unlock()
			s.logger.Warn("compaction error: failed to persist compaction revisions", "error", err)
			return nil, err
		}
		wtx.Unlock()
	}

	// Execute comapction
	c := make(chan struct{})
	compactedRev := s.compactedMainRev // snapshot before defer releases metaMu
	go func() {
		defer close(c)
		// Update finished schedulde compaction revision inside doCompact
		s.doCompact(rev, compactedRev)
	}()

	return c, nil
}

func (s *KvStore) doCompact(targetRev, compactedRev int64) {
	s.logger.Info("compaction started", "target_revision", targetRev)
	// 1) collect values we still should retain
	retain, err := s.kvIndex.Compact(targetRev)
	if err != nil {
		s.logger.Error("compaction error: failed to compact key index", "error", err, "target_revision", targetRev)
		return
	}

	// 2) delete en-masse entries where entry.modRev <= rev
	start := kv.EncodeRevisionAsBucketKey(kv.Revision{Main: compactedRev}, kv.NewRevBytes())
	endExcluded := kv.EncodeRevisionAsBucketKey(kv.Revision{Main: targetRev + 1}, kv.NewRevBytes())

	// batch deletes
	var (
		done           bool
		numDeleted     int
		lastRevVisited kv.Revision
		batch          = make([][]byte, 0, MAX_COMPACTION_BATCH_SIZE)
	)
	for {
		wtx := s.backend.WriteTx()
		wtx.Lock()
		batch := batch[:0]

		err := wtx.UnsafeScan(schema.BucketKV, start, endExcluded, func(k, v []byte) error {
			if len(batch) == MAX_COMPACTION_BATCH_SIZE {
				return fmt.Errorf("MAX_COMPACTION_BATCH_SIZE exceeded")
			}
			bk := kv.DecodeKvBucketKey(k)
			lastRevVisited = bk.Revision

			if _, shouldRetain := retain[bk.Revision]; !shouldRetain {
				batch = append(batch, append([]byte{}, k...))
			}
			return nil
		})

		if err != nil {
			s.logger.Error("compaction error: scanning returned an error", "error", err)
		}

		for _, k := range batch {
			if err := wtx.UnsafeDelete(schema.BucketKV, k); err != nil {
				s.logger.Error("compaction error: failed to delete key", "error", err)
			} else {
				numDeleted++
			}
		}

		if err == nil || len(batch) < MAX_COMPACTION_BATCH_SIZE {
			// were finished
			done = true
		}

		if done {
			// persist meta
			revBytes := kv.EncodeRevisionAsBucketKey(kv.Revision{Main: targetRev}, kv.NewRevBytes())
			if err := wtx.UnsafePut(schema.BucketMeta, schema.KeyFinishedCompactedRev, revBytes); err != nil {
				s.logger.Error("compaction error: failed to persist KeyCompactionFinished to meta bucket", "error", err)
			}
		}

		if _, err := wtx.Commit(); err != nil {
			s.logger.Error("compaction error: commit failed", "error", err)
			wtx.Rollback()
			wtx.Unlock()
			return
		}
		wtx.Unlock()

		if done {
			break
		}

		nextRev, ok := nextMainRevOverflowing(s.logger, lastRevVisited, 1)
		if ok {
			start = kv.EncodeRevisionAsBucketKey(nextRev, kv.NewRevBytes())
		} else {
			s.logger.Error("compaction error: attempted to increment revision past the end range (math.MaxInt64)")
			done = true
		}
	}

	s.metaMu.Lock()
	s.compactedMainRev = targetRev
	s.metaMu.Unlock()

	s.logger.Info("finished compaction", "revision", targetRev, "deleted_key_count", numDeleted)
}

func (s *KvStore) Ping() error {
	return s.backend.Ping()
}

func nextMainRevOverflowing(l *slog.Logger, r kv.Revision, delta int64) (rev kv.Revision, ok bool) {
	if r.Main > math.MaxInt64-delta {
		errMsg := fmt.Sprintf(
			"tried to add %d to current revision %d, but that would overflow int64",
			delta, r.Main,
		)
		l.Error(errMsg)
		return kv.Revision{}, false
	}
	return kv.Revision{
		Main: r.Main + delta,
		Sub:  r.Sub,
	}, true
}
