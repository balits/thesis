package mvcc

import (
	"bytes"
	"testing"

	"github.com/balits/kave/internal/schema"
	"github.com/balits/kave/internal/util"
	"github.com/stretchr/testify/require"
)

func Test_Writer_ReaderImpl_RangeSingleKey(t *testing.T) {
	t.Parallel()
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("foo"), []byte("bar"), 0)
	w.End()

	r := s.NewWriter()
	defer r.End()
	entries, total, lastRev, err := r.Range([]byte("foo"), nil, 0, 0)
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if total != 1 {
		t.Errorf("total = %d, want 1", total)
	}
	if lastRev != 1 {
		t.Errorf("curRev = %d, want 1", lastRev)
	}
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
	if !bytes.Equal(entries[0].Value, []byte("bar")) {
		t.Errorf("value = %q, want %q", entries[0].Value, "bar")
	}
}

func Test_Writer_ReaderImpl_RangeAtSpecificRev(t *testing.T) {
	t.Parallel()
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("foo"), []byte("v1"), 0)
	w.End()

	w = s.NewWriter()
	w.Put([]byte("foo"), []byte("v2"), 0)
	w.End()

	r := s.NewWriter()
	defer r.End()
	entries, _, _, err := r.Range([]byte("foo"), nil, 1, 0)
	if err != nil {
		t.Fatalf("Range at rev 1: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
	if !bytes.Equal(entries[0].Value, []byte("v1")) {
		t.Errorf("value at rev 1 = %q, want %q", entries[0].Value, "v1")
	}

	entries, _, _, err = r.Range([]byte("foo"), nil, 2, 0)
	if err != nil {
		t.Fatalf("Range at rev 2: %v", err)
	}
	if !bytes.Equal(entries[0].Value, []byte("v2")) {
		t.Errorf("value at rev 2 = %q, want %q", entries[0].Value, "v2")
	}
}

func Test_Writer_ReaderImpl_RangeRevZeroUsesCurrentRev(t *testing.T) {
	t.Parallel()
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("k"), []byte("v1"), 0)
	w.End()

	w = s.NewWriter()
	w.Put([]byte("k"), []byte("v2"), 0)
	w.End()

	r := s.NewWriter()
	defer r.End()
	entries, _, _, err := r.Range([]byte("k"), nil, 0, 0)
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if !bytes.Equal(entries[0].Value, []byte("v2")) {
		t.Errorf("rev=0 should return latest: got %q, want %q", entries[0].Value, "v2")
	}
}

func Test_Writer_ReaderImpl_RangeFutureRevError(t *testing.T) {
	t.Parallel()
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("k"), []byte("v"), 0)
	w.End()

	r := s.NewWriter()
	defer r.End()
	_, _, _, err := r.Range([]byte("k"), nil, 999, 0)
	if err == nil {
		t.Error("expected error for future revision")
	}
}

func Test_Writer_ReaderImpl_RangeMultipleKeys(t *testing.T) {
	t.Parallel()
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("a"), []byte("1"), 0)
	w.Put([]byte("b"), []byte("2"), 0)
	w.Put([]byte("c"), []byte("3"), 0)
	w.Put([]byte("d"), []byte("4"), 0)
	w.End()

	r := s.NewWriter()
	defer r.End()
	entries, total, _, err := r.Range([]byte("b"), []byte("d"), 0, 0)
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if total != 2 {
		t.Errorf("total = %d, want 2", total)
	}
	if len(entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(entries))
	}
}

func Test_Writer_ReaderImpl_RangeWithLimit(t *testing.T) {
	t.Parallel()
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("a"), []byte("1"), 0)
	w.Put([]byte("b"), []byte("2"), 0)
	w.Put([]byte("c"), []byte("3"), 0)
	w.End()

	r := s.NewWriter()
	defer r.End()
	entries, total, _, err := r.Range([]byte("a"), []byte("d"), 0, 2)
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("entries = %d, want 2 (limited)", len(entries))
	}
	if total != 3 {
		t.Errorf("total = %d, want 3 (unlimited count)", total)
	}
}

func Test_Writer_ReaderImpl_RangeNonExistentKey(t *testing.T) {
	t.Parallel()
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("foo"), []byte("bar"), 0)
	w.End()

	r := s.NewWriter()
	defer r.End()
	entries, total, _, err := r.Range([]byte("zzz"), nil, 0, 0)
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if len(entries) != 0 || total != 0 {
		t.Errorf("entries=%d total=%d, want 0 0", len(entries), total)
	}
}

func Test_Writer_ReaderImpl_RangeDeletedKey(t *testing.T) {
	t.Parallel()
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("foo"), []byte("bar"), 0)
	w.End()

	w = s.NewWriter()
	w.DeleteKey([]byte("foo"))
	w.End()

	r := s.NewWriter()
	defer r.End()
	entries, _, _, err := r.Range([]byte("foo"), nil, 0, 0)
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("deleted key should not appear: entries = %d", len(entries))
	}
}

func Test_Writer_ReaderImpl_RangeDeletedKeyAtOldRev(t *testing.T) {
	t.Parallel()
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("foo"), []byte("bar"), 0)
	w.End()

	w = s.NewWriter()
	w.DeleteKey([]byte("foo"))
	w.End()

	r := s.NewWriter()
	defer r.End()
	entries, _, _, err := r.Range([]byte("foo"), nil, 1, 0)
	if err != nil {
		t.Fatalf("Range at old rev: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("should see key at rev before delete: entries = %d", len(entries))
	}
}

func Test_Writer_ReaderImpl_RangeEmptyStore(t *testing.T) {
	t.Parallel()
	s := newTestKVStore(t)
	defer s.backend.Close()

	r := s.NewWriter()
	defer r.End()
	entries, total, _, err := r.Range([]byte("anything"), nil, 0, 0)
	if err != nil {
		t.Fatalf("Range on empty store: %v", err)
	}
	if len(entries) != 0 || total != 0 {
		t.Errorf("empty store: entries=%d total=%d", len(entries), total)
	}
}

func Test_Writer_PutSingleKey(t *testing.T) {
	t.Parallel()
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	err := w.Put([]byte("foo"), []byte("bar"), 0)
	require.NoError(t, err, "unexpected error from Put()")
	w.End()

	rev, changes := w.UnsafeExpectedChanges()
	require.Equal(t, int64(1), rev, "revision = %d, want 1", rev)
	require.Len(t, changes, 1, "changes = %d, want 1", len(changes))
}

func Test_Writer_PutMultipleKeys(t *testing.T) {
	t.Parallel()
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("a"), []byte("1"), 0)
	w.Put([]byte("b"), []byte("2"), 0)
	w.Put([]byte("c"), []byte("3"), 0)
	w.End()

	rev, changes := w.UnsafeExpectedChanges()
	require.Equal(t, rev, int64(1), "revision = %d, want 1 (single writer = single main rev)", rev)
	require.Len(t, changes, 3, "changes = %d, want 3", len(changes))
}

func Test_Writer_PutSameKeyTwice(t *testing.T) {
	t.Parallel()
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("k"), []byte("v1"), 0)
	w.End()

	w = s.NewWriter()
	w.Put([]byte("k"), []byte("v2"), 0)
	w.End()

	rev, _ := w.UnsafeExpectedChanges()
	require.Equal(t, rev, int64(2), "revision = %d, want 2", rev)
}

func Test_Writer_PutPreservesCreateRev(t *testing.T) {
	t.Parallel()
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("k"), []byte("v1"), 0)
	w.End()

	w = s.NewWriter()
	w.Put([]byte("k"), []byte("v2"), 0)
	w.End()

	_, changes := w.UnsafeExpectedChanges()
	require.Len(t, changes, 1, "changes = %d, want 1", len(changes))
	require.Equal(t, int64(1), changes[0].CreateRev, "CreateRev = %d, want 1 (should be preserved from first put)", changes[0].CreateRev)
	require.Equal(t, int64(2), changes[0].Version, "Version = %d, want 2", changes[0].Version)
}

func Test_Writer_PutEntryFields(t *testing.T) {
	t.Parallel()
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("mykey"), []byte("myval"), 0)
	w.End()

	_, changes := w.UnsafeExpectedChanges()
	require.Len(t, changes, 1, "changes = %d, want 1", len(changes))
	e := changes[0]
	require.Equal(t, []byte("mykey"), e.Key, "Key = %q, want %q", e.Key, "mykey")
	require.Equal(t, []byte("myval"), e.Value, "Value = %q, want %q", e.Value, "myval")
	require.Equal(t, int64(1), e.CreateRev, "CreateRev = %d, want 1", e.CreateRev)
	require.Equal(t, int64(1), e.ModRev, "ModRev = %d, want 1", e.ModRev)
	require.Equal(t, int64(1), e.Version, "Version = %d, want 1", e.Version)
}

func Test_Writer_Expected_Changes_ReturnsEndRev(t *testing.T) {
	t.Parallel()
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("a"), []byte("1"), 0)
	w.End()

	endRev, _ := w.UnsafeExpectedChanges()
	require.Equal(t, int64(1), endRev, "start rev = %d, want 1", endRev)
}

func Test_Writer_DeleteKey(t *testing.T) {
	t.Parallel()
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("foo"), []byte("bar"), 0)
	w.End()

	w = s.NewWriter()
	err := w.DeleteKey([]byte("foo"))
	require.NoError(t, err, "unexpected error from DeleteKey()")
	w.End()

	rev, changes := w.UnsafeExpectedChanges()
	require.Equal(t, int64(2), rev, "rev = %d, want 2", rev)
	require.Len(t, changes, 1, "changes = %d, want 1", len(changes))
	require.Equal(t, []byte("foo"), changes[0].Key, "deleted key = %q, want %q", changes[0].Key, "foo")
	require.True(t, changes[0].Tombstone(), "deleted entry should be a tombstone")
}

func Test_Writer_DeleteKeyNonExistent(t *testing.T) {
	t.Parallel()
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	err := w.DeleteKey([]byte("nope"))
	require.NoError(t, err, "unexpected error from DeleteKey()")
	w.End()

	rev, changes := w.UnsafeExpectedChanges()
	require.Equal(t, int64(1), rev, "rev = %d, want 1", rev)
	require.Len(t, changes, 0, "changes = %d, want 0", len(changes))
}

func Test_Writer_DeleteKeyThenReCreate(t *testing.T) {
	t.Parallel()
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("foo"), []byte("v1"), 0)
	w.End()

	w = s.NewWriter()
	w.DeleteKey([]byte("foo"))
	w.End()

	w = s.NewWriter()
	w.Put([]byte("foo"), []byte("v2"), 0)
	w.End()

	rev, changes := w.UnsafeExpectedChanges()
	require.Equal(t, int64(3), rev, "rev = %d, want 3", rev)
	require.Len(t, changes, 1, "changes = %d, want 1", len(changes))
	require.Equal(t, int64(3), changes[0].CreateRev, "re-created key CreateRev = %d, want 3", changes[0].CreateRev)
	require.Equal(t, int64(1), changes[0].Version, "re-created key Version = %d, want 1", changes[0].Version)
}

func Test_Writer_DeleteRange(t *testing.T) {
	t.Parallel()
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("a"), []byte("1"), 0)
	w.Put([]byte("b"), []byte("2"), 0)
	w.Put([]byte("c"), []byte("3"), 0)
	w.Put([]byte("d"), []byte("4"), 0)
	w.End()

	w = s.NewWriter()
	err := w.DeleteRange([]byte("b"), []byte("d"))
	require.NoError(t, err, "unexpected error from DeleteRange()")
	w.End()
	rev, ch := w.UnsafeExpectedChanges()
	require.Equal(t, int64(2), rev, "rev = %d, want 2", rev)
	require.Len(t, ch, 2, "deleted = %d, want 2", len(ch))
}

func Test_Writer_DeleteRangeEmpty(t *testing.T) {
	t.Parallel()
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("a"), []byte("1"), 0)
	w.End()

	w = s.NewWriter()
	err := w.DeleteRange([]byte("x"), []byte("z"))
	require.NoError(t, err, "unexpected error from DeleteRange()")
	w.End()
	_, ch := w.UnsafeExpectedChanges()
	require.Len(t, ch, 0, "deleted = %d, want 0", len(ch))
}

func Test_Writer_UnsafeExpectedChanges_Empty_After_EmptyWrite(t *testing.T) {
	t.Parallel()
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.End()

	r, ch := w.UnsafeExpectedChanges()
	require.Equal(t, r, int64(1)) // expectedRev is always going to be 1 + w.startRev (which is 0 here)
	require.Len(t, ch, 0)
}

func Test_Writer_UnsafeExpectedChanges_IncludesTombstones(t *testing.T) {
	t.Parallel()
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("foo"), []byte("bar"), 0)
	w.End()

	w = s.NewWriter()
	w.DeleteKey([]byte("foo"))
	w.End()

	_, changes := w.UnsafeExpectedChanges()
	if len(changes) != 1 {
		t.Fatalf("changes = %d, want 1", len(changes))
	}
	if !bytes.Equal(changes[0].Key, []byte("foo")) {
		t.Errorf("tombstone key = %q, want %q", changes[0].Key, "foo")
	}
}

func Test_Writer_End_NoChangesNoRevBump(t *testing.T) {
	t.Parallel()
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.End()

	currRev, _ := s.Revisions()
	if currRev.Main != 0 {
		t.Errorf("revision = %d, want 0 (no changes = no bump)", currRev.Main)
	}
}

func Test_Writer_End_BumpsRevisionOnce(t *testing.T) {
	t.Parallel()
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("a"), []byte("1"), 0)
	w.Put([]byte("b"), []byte("2"), 0)
	w.Put([]byte("c"), []byte("3"), 0)
	w.End()

	currRev, _ := s.Revisions()
	if currRev.Main != 1 {
		t.Errorf("revision = %d, want 1 (one writer = one bump)", currRev.Main)
	}
}

func Test_Writer_End_PersistsRaftMeta(t *testing.T) {
	t.Parallel()
	s := newTestKVStore(t)
	defer s.backend.Close()

	s.UpdateInmemRaftMeta(42, 7)

	w := s.NewWriter()
	w.Put([]byte("k"), []byte("v"), 0)
	w.End()

	rtx := s.backend.ReadTx()
	rtx.RLock()
	indexBytes, _ := rtx.UnsafeGet(schema.BucketMeta, schema.KeyRaftApplyIndex)
	termBytes, _ := rtx.UnsafeGet(schema.BucketMeta, schema.KeyRaftTerm)
	rtx.RUnlock()

	idx, _ := util.DecodeUint64(indexBytes)
	term, _ := util.DecodeUint64(termBytes)

	if idx != 42 {
		t.Errorf("raft index = %d, want 42", idx)
	}
	if term != 7 {
		t.Errorf("raft term = %d, want 7", term)
	}
}

func Test_Writer_End_NoRaftMetaWhenZero(t *testing.T) {
	t.Parallel()
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("k"), []byte("v"), 0)
	w.End()

	rtx := s.backend.ReadTx()
	rtx.RLock()
	indexBytes, _ := rtx.UnsafeGet(schema.BucketMeta, schema.KeyRaftApplyIndex)
	rtx.RUnlock()

	if indexBytes != nil {
		t.Error("raft meta should not be persisted when raftIndex == 0")
	}
}

func Test_Writer_Abort_RollsBackIndex(t *testing.T) {
	t.Parallel()
	store := newTestKVStore(t)

	// 1. Setup initial state
	w1 := store.NewWriter()
	require.NoError(t, w1.Put([]byte("foo"), []byte("v1"), 0))
	require.NoError(t, w1.End())

	// 2. Start a new transaction that makes a mess
	w2 := store.NewWriter()

	// Modify existing key
	require.NoError(t, w2.Put([]byte("foo"), []byte("v2"), 0))
	// Add new key
	require.NoError(t, w2.Put([]byte("bar"), []byte("v1"), 0))
	// Delete the existing key
	require.NoError(t, w2.DeleteKey([]byte("foo")))

	// 3. Verify Intra-transaction visibility (the writer should see its own changes)
	barEntry := w2.Get([]byte("bar"), 0)
	require.NotNil(t, barEntry)
	require.Equal(t, "v1", string(barEntry.Value))

	fooEntry := w2.Get([]byte("foo"), 0)
	require.Nil(t, fooEntry, "foo should be tombstoned inside the transaction")

	// 4. ABORT!
	w2.Abort()

	// 5. Verify Rollback (Global state must be completely untouched)
	r := store.NewReader()

	// "bar" should be completely gone
	barRestored := r.Get([]byte("bar"), 0)
	require.Nil(t, barRestored, "bar should have been rolled back from the tree index")

	// "foo" should be exactly as it was before Txn 2
	fooRestored := r.Get([]byte("foo"), 0)
	require.NotNil(t, fooRestored, "foo's tombstone should be rolled back")
	require.Equal(t, "v1", string(fooRestored.Value), "foo should revert to v1")
}

func Test_Writer_Abort_MultipleUpdatesToSameKey(t *testing.T) {
	t.Parallel()
	store := newTestKVStore(t)

	w := store.NewWriter()

	// Update the exact same key 3 times in one transaction
	// This tests if the undo loop correctly drops all 3 sub-revisions
	// without breaking the keyIndex generations
	require.NoError(t, w.Put([]byte("spam"), []byte("1"), 0))
	require.NoError(t, w.Put([]byte("spam"), []byte("2"), 0))
	require.NoError(t, w.Put([]byte("spam"), []byte("3"), 0))

	// Ensure it's there
	require.NotNil(t, w.Get([]byte("spam"), 0))

	w.Abort()

	// Ensure the keyIndex is completely empty and removed from the B-Tree
	r := store.NewReader()
	require.Nil(t, r.Get([]byte("spam"), 0))
}

func Test_Writer_Abort_ProtectsGlobalRevision(t *testing.T) {
	t.Parallel()
	store := newTestKVStore(t)

	startRev, _ := store.Revisions()

	w := store.NewWriter()
	require.NoError(t, w.Put([]byte("foo"), []byte("bar"), 0))
	w.Abort()

	endRev, _ := store.Revisions()
	require.Equal(t, startRev.Main, endRev.Main)
}

func Test_Writer_KeyCount_IncreasesOnNewKeys(t *testing.T) {
	t.Parallel()
	s := newTestKVStore(t)
	defer s.backend.Close()

	require.Equal(t, int64(0), s.KeyCount())

	w := s.NewWriter()
	w.Put([]byte("a"), []byte("1"), 0)
	w.Put([]byte("b"), []byte("2"), 0)
	w.Put([]byte("c"), []byte("3"), 0)
	require.NoError(t, w.End())

	require.Equal(t, int64(3), s.KeyCount(), "three new keys should increase count to 3")
}

func Test_Writer_KeyCount_UnchangedOnUpdate(t *testing.T) {
	t.Parallel()
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("k"), []byte("v1"), 0)
	require.NoError(t, w.End())
	require.Equal(t, int64(1), s.KeyCount())

	w = s.NewWriter()
	w.Put([]byte("k"), []byte("v2"), 0)
	require.NoError(t, w.End())

	require.Equal(t, int64(1), s.KeyCount(), "updating existing key should not change count")
}

func Test_Writer_KeyCount_DecreasesOnDelete(t *testing.T) {
	t.Parallel()
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("a"), []byte("1"), 0)
	w.Put([]byte("b"), []byte("2"), 0)
	require.NoError(t, w.End())
	require.Equal(t, int64(2), s.KeyCount())

	w = s.NewWriter()
	w.DeleteKey([]byte("a"))
	require.NoError(t, w.End())

	require.Equal(t, int64(1), s.KeyCount(), "deleting one key should decrease count by 1")
}
