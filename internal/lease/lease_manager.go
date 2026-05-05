package lease

import (
	"container/heap"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/mvcc"
	"github.com/balits/kave/internal/schema"
	"github.com/balits/kave/internal/storage/backend"
	"github.com/balits/kave/internal/util"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	errLease           = errors.New("lease error")
	ErrLeaseIdZero     = fmt.Errorf("%w: lease ID cannot be zero", errLease)
	ErrLeaseIDConflict = fmt.Errorf("%w: lease ID conflict", errLease)
	ErrLeaseNotFound   = fmt.Errorf("%w: lease not found", errLease)
	ErrLeaseInvalidTTL = fmt.Errorf("%w: TTL cannot be negative", errLease)
)

const (
	MAX_BATCH_SIZE = 1
)

type LeaseManager struct {
	rwlock   sync.RWMutex
	clock    util.Clock
	leaseMap map[int64]*Lease
	heap     heap.Interface
	heapMap  map[int64]*heapItem
	store    *mvcc.KvStore
	backend  backend.Backend
	metrics  *metrics.LeaseMetrics
	logger   *slog.Logger
}

func NewManager(reg prometheus.Registerer, logger *slog.Logger, store *mvcc.KvStore, backend backend.Backend) *LeaseManager {
	h := NewLeaseHeap()
	heap.Init(&h)
	m := &LeaseManager{
		clock:    util.NewRealClock(),
		leaseMap: make(map[int64]*Lease),
		heap:     &h,
		heapMap:  make(map[int64]*heapItem),
		store:    store,
		backend:  backend,
		logger:   logger.With("component", "lease_manager"),
	}
	activeLeasesFunc := func() int {
		m.rwlock.RLock()
		defer m.rwlock.RUnlock()
		return len(m.leaseMap)
	}
	m.metrics = metrics.NewLeaseMetrics(reg, activeLeasesFunc)
	return m
}

func (lm *LeaseManager) Grant(id, ttl int64) (*Lease, error) {
	if id == 0 {
		return nil, ErrLeaseIdZero
	}
	if ttl <= 0 {
		return nil, ErrLeaseInvalidTTL
	}
	if ttl > maxTTL {
		ttl = maxTTL
	}

	start := time.Now()
	defer func() { lm.metrics.GrantDurationSec.Observe(time.Since(start).Seconds()) }()

	// new: id is not generated at the leaseManager (inside the FSM)
	// but the service layer, proposing the GrantCmd AND the id with it.
	// This way each nodes fsm is replicating the GrantCmd with the same ID,
	// as opposed to granting a lease, but generating unique IDS per node
	lease := newLease(id, ttl, lm.clock)

	lm.rwlock.Lock()
	defer lm.rwlock.Unlock()

	if err := lm.unsafeSaveToLeaseMap(lease); err != nil {
		return nil, err
	}
	lm.unsafePushToHeap(lease)
	if err := lm.unsafePersistToBackend(lease); err != nil {
		return nil, err
	}

	lm.metrics.LeasesGranted.Inc()
	lm.logger.WithGroup("lease").
		Info("lease granted",
			"id", lease.ID,
			"ttl", lease.TTL,
			"expiry", lease.expiry,
		)
	return lease, nil
}

// FIXME: is delete + writer End() error handled well?
func (lm *LeaseManager) Revoke(id int64) (found, revoked bool, err error) {
	lm.rwlock.Lock()
	defer lm.rwlock.Unlock()

	start := time.Now()
	defer func() { lm.metrics.RevokeDurationSec.Observe(time.Since(start).Seconds()) }()

	lease, ok := lm.leaseMap[id]
	if !ok {
		return false, false, ErrLeaseNotFound
	}
	found = ok

	lease.keysMu.Lock()
	toDelete := make([][]byte, 0, len(lease.keySet))
	for lk := range lease.keySet {
		toDelete = append(toDelete, []byte(lk))
		delete(lease.keySet, lk)
	}
	lease.keysMu.Unlock()

	if len(toDelete) > 0 {
		w := lm.store.NewWriter()
		actuallyDeleted := 0
		for _, k := range toDelete {
			if err := w.DeleteKey(k); err != nil {
				w.Abort()
				msg := fmt.Sprintf("lease revoke: failed to delete key %s", k)
				lm.logger.Error(msg,
					"key", k,
					"id", lease.ID,
					"error", err,
				)
				return true, false, fmt.Errorf("%s: %w", msg, err)
			} else {
				actuallyDeleted++
			}
		}

		if err := w.End(); err != nil {
			w.Abort()
			msg := "lease revoke: failed to remove attached keys: writer.End() failed"
			lm.logger.Error(msg,
				"error", err,
				"id", lease.ID,
				"all_key_count", len(toDelete),
				"deleted_key_count", actuallyDeleted,
			)
			return false, false, fmt.Errorf("%s: %w", msg, err)
		} else {
			lm.logger.Info("lease revoke: removed attached keys",
				"id", lease.ID,
				"all_key_count", len(toDelete),
				"deleted_key_count", actuallyDeleted,
			)
			lm.metrics.KeysPerRevoke.Observe(float64(len(toDelete)))
		}
	}

	lm.unsafeRemoveFromLeaseMap(lease)
	lm.unsafeRemoveFromHeap(lease.ID)

	// logoljuk, de nem kell hibával visszatértünk:
	// a kulcsokat már úgyis töröltük, és a régi lease-k úgyis törlődnek
	// a következő Restorenál
	if err := lm.unsafeRemoveFromBackend(lease); err != nil {
		lm.logger.Warn("lease revoke: failed to delete lease from backend",
			"id", id,
			"error", err,
		)
	}

	revoked = true
	lm.metrics.LeasesRevoked.Inc()
	lm.logger.WithGroup("lease").
		Info("lease revoked successfuly",
			"id", lease.ID,
		)
	return
}

func (lm *LeaseManager) KeepAlive(id int64) (remainingTTL int64, err error) {
	if id == 0 {
		return 0, ErrLeaseIdZero
	}

	lm.rwlock.Lock()
	defer lm.rwlock.Unlock()

	start := time.Now()
	defer func() { lm.metrics.KeepAliveDurationSec.Observe(time.Since(start).Seconds()) }()

	lease, ok := lm.leaseMap[id]
	if !ok {
		lm.metrics.KeepAliveErrors.Inc()
		return 0, ErrLeaseNotFound
	}

	newExpiry := lm.clock.Now().Add(time.Second * time.Duration(lease.TTL))
	remTTL := int64(lm.clock.Until(newExpiry).Seconds())
	lease.Update(remTTL, newExpiry)

	lm.unsafeUpdateHeap(id, newExpiry)
	if err = lm.unsafePersistToBackend(lease); err != nil {
		lm.metrics.KeepAliveErrors.Inc()
		// logoljuk, de nem kell hibával visszatértünk:
		// a következő checkpointnál újrapróbálkozunk
		lm.logger.Warn("lease keepalive: persist failed, will retry on checkpoint",
			"id", id,
			"error", err,
		)
	}
	lm.metrics.KeepAliveTotal.Inc()
	lm.logger.WithGroup("lease").
		Info("lease kept alive",
			"id", id,
		)
	return remTTL, nil
}

func (lm *LeaseManager) AttachKey(id int64, key []byte) {
	lm.rwlock.Lock()
	defer lm.rwlock.Unlock()
	lm.unsafeAttachKey(id, key)
	lm.metrics.LeasedKeys.Inc()
}

func (lm *LeaseManager) DetachKey(id int64, key []byte) {
	lm.rwlock.Lock()
	defer lm.rwlock.Unlock()
	lease, ok := lm.leaseMap[id]
	if !ok {
		return
	}
	lease.DetachKey(key)
	lm.metrics.LeasedKeys.Dec()
}

func (lm *LeaseManager) Lookup(id int64) (*Lease, error) {
	lm.rwlock.RLock()
	defer lm.rwlock.RUnlock()

	l, ok := lm.leaseMap[id]
	if !ok {
		return nil, ErrLeaseNotFound
	}
	return l, nil
}

func (lm *LeaseManager) Checkpoint() []command.Checkpoint {
	lm.rwlock.RLock()
	defer lm.rwlock.RUnlock()

	checks := make([]command.Checkpoint, 0, len(lm.leaseMap))
	for id, lease := range lm.leaseMap {
		rem := lease.RemainingSec(lm.clock)
		if rem <= 0 {
			continue
		}
		checks = append(checks, command.Checkpoint{
			LeaseID:      id,
			RemainingTTL: rem,
		})
	}
	return checks
}

func (lm *LeaseManager) ApplyCheckpoint(cmd command.CmdLeaseCheckpoint) {
	lm.rwlock.Lock()
	defer lm.rwlock.Unlock()

	for _, ck := range cmd.Checkpoints {
		lease, ok := lm.leaseMap[ck.LeaseID]
		if !ok {
			continue
		}
		if ck.RemainingTTL <= 0 {
			continue
		}

		lease.Update(ck.RemainingTTL, lease.expiry)
		if err := lm.unsafePersistToBackend(lease); err != nil {
			// logoljuk, de nem kell hibával visszatértünk:
			// a következő checkpointnál újrapróbálkozunk
			// legrosszabb esetben ez a lease picit tovább él
			// a következő restore után
			lm.logger.Warn("checkpoint failed: failed to persist lease",
				"lease_id", lease.ID,
				"checkpoint_remainging_ttl_sec", ck.RemainingTTL,
				"error", err,
			)
		}
	}
}

func (lm *LeaseManager) DrainExpiredLeases() []*Lease {
	lm.rwlock.Lock()
	defer lm.rwlock.Unlock()
	now := lm.clock.Now()
	expired := make([]*Lease, 0)
	for {
		min := lm.heap.(*leaseHeap).peekMin()
		if min == nil {
			break
		}

		if min.expiry.Before(now) {
			item := heap.Pop(lm.heap).(*heapItem)
			delete(lm.heapMap, item.lease.ID)
			expired = append(expired, item.lease)
		} else {
			break
		}
	}
	return expired
}

func (lm *LeaseManager) ApplyExpired(cmd command.CmdLeaseExpire) (*command.ResultLeaseExpire, error) {
	lm.rwlock.Lock()
	defer lm.rwlock.Unlock()
	start := time.Now()
	defer func() { lm.metrics.ApplyExpiredDurationSec.Observe(time.Since(start).Seconds()) }()

	attachedKeys := make([][]byte, 0)
	leaseCount := 0
	for _, id := range cmd.ExpiredIDs {
		l, ok := lm.leaseMap[id]
		if !ok {
			continue
		} else {
			leaseCount++
		}

		l.keysMu.RLock()
		for k := range l.keySet {
			attachedKeys = append(attachedKeys, []byte(k))
		}
		l.keysMu.RUnlock()

		lm.unsafeRemoveFromLeaseMap(l)
		// DrainExpiredLeases() already removes it from heap and heapMap
		// lm.unsafeRemoveFromHeap(l.ID)
	}

	w := lm.store.NewWriter()
	for _, key := range attachedKeys {
		err := w.DeleteKey(key)
		if err != nil {
			w.Abort()
			lm.logger.Error("apply expired: failed to remove key from store",
				"error", err,
				"key", key,
			)
			return nil, err
		}
	}
	_ = w.End()
	lm.metrics.KeysPerExpiry.Observe(float64(len(attachedKeys)))
	lm.logger.Debug("apply expired: removed keys from store", "key_count", len(attachedKeys))

	wtx := lm.backend.WriteTx()
	wtx.Lock()
	defer wtx.Unlock()
	for _, id := range cmd.ExpiredIDs {
		bk := LeaseBucketKey(id)
		if err := wtx.UnsafeDelete(schema.BucketLease, EncodeLeaseBucketKey(bk)); err != nil {
			lm.logger.Error("apply expired: failed to remove lease",
				"error", err,
				"lease_id", id,
			)
			wtx.Rollback()
			return nil, err
		}
	}

	if _, err := wtx.Commit(); err != nil {
		lm.logger.Error("apply expired: failed to commit deleted keys", "error", err)
		wtx.Rollback()
		return nil, err
	}

	lm.metrics.LeasesExpired.Add(float64(leaseCount))
	lm.logger.Info("apply expired: removed leases from backend",
		"lease_count", leaseCount,
		"key_count", len(attachedKeys),
	)

	return &command.ResultLeaseExpire{
		RemovedLeaseCount: leaseCount,
		RemovedKeyCount:   len(attachedKeys),
	}, nil
}

func (lm *LeaseManager) Restore() error {
	lm.rwlock.Lock()
	defer lm.rwlock.Unlock()

	startTime := time.Now()
	defer func() { lm.metrics.RestoreDurationSec.Observe(time.Since(startTime).Seconds()) }()

	clear(lm.leaseMap)
	clear(lm.heapMap)
	newHeap := NewLeaseHeap()
	heap.Init(&newHeap)
	lm.heap = &newHeap

	type leaseRestore struct {
		id           int64
		remainingTTL int64
	}

	// regrant leases
	var (
		leaseDone                        = false
		leaseCountTotal                  = 0
		leaseBatch                       = make([]leaseRestore, 0)
		startLeaseBucket          []byte = nil
		lastLeaseBucketKeyVisited        = LeaseBucketKey(math.MinInt64)
	)

	for !leaseDone {
		leaseBatch = leaseBatch[:0]
		leaseCountTotal = 0

		rtx := lm.backend.ReadTx()
		rtx.RLock()
		// only return error on MAX_BATCH_SIZE
		errBatchExceeded := rtx.UnsafeScan(schema.BucketLease, startLeaseBucket, nil, func(k, v []byte) error {
			if len(leaseBatch) == MAX_BATCH_SIZE {
				return errors.New("batch size limit exceeded")
			}
			lastLeaseBucketKeyVisited = DecodeLeaseBucketKey(k)

			l, err := DecodeLease(v)
			if err != nil {
				lm.logger.Warn("restore error: failed to decode lease", "error", err)
				return nil
			}

			if l.remainingTTL <= 0 {
				return nil
			}

			leaseBatch = append(leaseBatch, leaseRestore{l.ID, l.remainingTTL})
			return nil
		})
		rtx.RUnlock()

		if errBatchExceeded == nil || lastLeaseBucketKeyVisited == math.MaxInt64 {
			leaseDone = true
		}

		if !leaseDone && (len(leaseBatch) > 0 || lastLeaseBucketKeyVisited != LeaseBucketKey(math.MinInt64)) {
			startLeaseBucket = EncodeLeaseBucketKey(lastLeaseBucketKeyVisited + 1)
		}

		for _, l := range leaseBatch {
			_, err := lm.unsafeRegrant(l.id, l.remainingTTL)
			if err != nil {
				lm.logger.Warn("restore error: failed to re-grant lease",
					"error", err,
					"lease_id", l.id,
				)
			}
		}

		leaseCountTotal += len(leaseBatch)
	}

	// reattach keys to leases
	var (
		keyDone                       = false
		keysVisited                   = 0
		keyCountTotal                 = 0
		keyBatch                      = make(map[int64][][]byte)
		startKvBucket          []byte = nil
		lastKvBucketKeyVisited kv.KvBucketKey
	)

	for !keyDone {
		clear(keyBatch)
		keysVisited = 0

		rtx := lm.backend.ReadTx()
		rtx.RLock()
		errBatchExceeded := rtx.UnsafeScan(schema.BucketKV, startKvBucket, nil, func(k, v []byte) error {
			if keysVisited == MAX_BATCH_SIZE {
				return errors.New("batch size limit exceeded")
			}
			keysVisited++

			bk := kv.DecodeKvBucketKey(k)
			lastKvBucketKeyVisited = bk
			if bk.Tombstone {
				lm.logger.Debug("restore: skipping tombstone key",
					"main", bk.Main,
					"sub", bk.Sub,
				)
				return nil
			}

			entry, err := kv.DecodeEntry(v)
			if err != nil {
				lm.logger.Warn("restore error: failed to decode entry", "error", err)
				return nil
			}
			if entry.LeaseID != 0 {
				keyBatch[entry.LeaseID] = append(keyBatch[entry.LeaseID], entry.Key)
			}
			return nil
		})
		rtx.RUnlock()

		if errBatchExceeded == nil {
			keyDone = true
		} else if lastKvBucketKeyVisited.Revision.Main == math.MaxInt64 {
			keyDone = true
		} else {
			nextRev, ok := nextFullRevisionOverflowing(lastKvBucketKeyVisited.Revision)
			if ok {
				startKvBucket = kv.EncodeRevisionAsBucketKey(nextRev, kv.NewRevBytes())
			} else {
				panic("restore error: attempted to increment revision past the end range (math.MaxInt64) EVEN AFTER guarding against it")
			}
		}

		for id, keys := range keyBatch {
			for _, k := range keys {
				lm.unsafeAttachKey(id, k)
			}
			keyCountTotal += len(keys)
		}
	}

	lm.logger.Info("restore: restored leases from backend",
		"regranted_lease_count", leaseCountTotal,
		"reattached_key_count", keyCountTotal,
	)

	// as this isnt a gaugeFunc it needs to be reset on restore
	lm.metrics.LeasedKeys.Set(float64(keyCountTotal))

	return nil
}

// privát helper függvények

// saveLeaseInmem elmenti a leaset a leaseMap-be
// és hozzáadja a heap-hez, hogy tudjuk mikor jár le
// errLeaseIDConflictot dob vissza, ha már van ilyen ID-val lease
// NOTE: caller needs to hold the lock
func (lm *LeaseManager) unsafeSaveToLeaseMap(l *Lease) error {
	if _, ok := lm.leaseMap[l.ID]; ok {
		return ErrLeaseIDConflict
	}
	lm.leaseMap[l.ID] = l
	return nil
}

// NOTE: caller needs to hold the lock
func (lm *LeaseManager) unsafePushToHeap(lease *Lease) {
	item := &heapItem{lease: lease, expiry: lease.expiry}
	heap.Push(lm.heap, item)
	lm.heapMap[lease.ID] = item
}

// unsafeRemoveFromHeap eltávolítja a least a heapről O(log n) időben
//
// NOTE: caller needs to hold the lock
func (lm *LeaseManager) unsafeRemoveFromHeap(id int64) {
	item, ok := lm.heapMap[id]
	if !ok {
		return
	}
	heap.Remove(lm.heap, item.index)
	delete(lm.heapMap, id)
}

// unsafePersistLease elmenti a leaset a backendbe
//
// NOTE: caller needs to hold the lock
func (lm *LeaseManager) unsafePersistToBackend(lease *Lease) error {
	bucketKey := EncodeLeaseBucketKey(LeaseBucketKey(lease.ID))
	leaseBytes, err := EncodeLease(lease)
	if err != nil {
		return fmt.Errorf("%w: %e", errLease, err)
	}

	wtx := lm.backend.WriteTx()
	wtx.Lock()
	defer wtx.Unlock()
	if err := wtx.UnsafePut(schema.BucketLease, bucketKey, leaseBytes); err != nil {
		wtx.Rollback()
		return fmt.Errorf("%v: %w", errLease, err)
	}
	if _, err := wtx.Commit(); err != nil {
		wtx.Rollback()
		return fmt.Errorf("%v: %w", errLease, err)
	}

	return nil
}

// NOTE: caller needs to hold the lock
func (lm *LeaseManager) unsafeRemoveFromBackend(lease *Lease) error {
	bucketKey := EncodeLeaseBucketKey(LeaseBucketKey(lease.ID))

	wtx := lm.backend.WriteTx()
	wtx.Lock()
	defer wtx.Unlock()
	if err := wtx.UnsafeDelete(schema.BucketLease, bucketKey); err != nil {
		wtx.Rollback()
		return fmt.Errorf("%v: %w", errLease, err)
	}
	if _, err := wtx.Commit(); err != nil {
		wtx.Rollback()
		return fmt.Errorf("%v: %w", errLease, err)
	}

	return nil
}

// unsafeRemoveFromLeaseMap törli a leaset a leaseMap-ből
//
// NOTE: caller needs to hold the lock
func (lm *LeaseManager) unsafeRemoveFromLeaseMap(l *Lease) {
	delete(lm.leaseMap, l.ID)
}

// unsafeUpdateHeap frissít egy már meglévő elemet
// heap.Fix O(log n) időben újraépíti a heap invaránsát
//
// NOTE: caller needs to hold the lock
func (lm *LeaseManager) unsafeUpdateHeap(id int64, t time.Time) {
	item, ok := lm.heapMap[id]
	if !ok {
		return
	}
	item.expiry = t
	heap.Fix(lm.heap, item.index)
}

// unsafeRegrant újra létrehozza a leaset, lockolás nélkül és mégfontosabb hogy
// az utolsó checkpoint utáni remainingTTL-t állítja be ttl-nek és expirynek,
// hogy ne kapjon egy lease se extra időt
//
// NOTE: caller needs to hold the lock
func (lm *LeaseManager) unsafeRegrant(id, remainingTTLsec int64) (*Lease, error) {
	if remainingTTLsec <= 0 {
		return nil, ErrLeaseInvalidTTL
	}
	if remainingTTLsec > maxTTL {
		remainingTTLsec = maxTTL
	}

	// this genuienly should not happen,
	// but be defensive just in case
	if id == 0 {
		return nil, fmt.Errorf("tried to reegrant lease with ID = 0")
	}

	lease := newLease(id, remainingTTLsec, lm.clock)

	if err := lm.unsafeSaveToLeaseMap(lease); err != nil {
		return nil, err
	}
	lm.unsafePushToHeap(lease)
	if err := lm.unsafePersistToBackend(lease); err != nil {
		return nil, err
	}

	lm.metrics.LeasesGranted.Inc()
	lm.logger.Debug("lease re-granted",
		"id", lease.ID,
		"ttl", lease.TTL,
		"expiry", lease.expiry,
	)
	return lease, nil
}

// NOTE: caller needs to hold the lock
func (lm *LeaseManager) unsafeAttachKey(id int64, key []byte) {
	lease, ok := lm.leaseMap[id]
	if !ok {
		return
	}
	lease.AttachKey(key)
}

func nextFullRevisionOverflowing(r kv.Revision) (kv.Revision, bool) {
	if r.Sub < math.MaxInt64 {
		return kv.Revision{
			Main: r.Main,
			Sub:  r.Sub + 1,
		}, true
	}

	if r.Main < math.MaxInt64 {
		return kv.Revision{
			Main: r.Main + 1,
			Sub:  0,
		}, true
	}

	return kv.Revision{}, false
}
