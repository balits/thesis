[![CI](https://github.com/balits/kave/actions/workflows/ci.yaml/badge.svg)](https://github.com/balits/kave/actions/workflows/ci.yaml)

Hello world
//elmelteti leiras + felhasznali doku + fejlesztesi doku (tesztelesi doksi + vhol template)

# Notes
- eventual consistency -> Leader only GET, no VerfiyLeader()
- strong consistency -> Leader only GET, VerifyLeader()
- dirty reads -> Leader, follower GET, no VerifyLeader()

# TODO
- [x] simplify http server handlers
- [x] prune random string(bytes) and []byte(string)
- [x] bytestrore.Defragment
- [ ] raft index -> mvcc 
    - [x] Global monotonic revision
    - ~~[x] Snapshot isolation~~
        - snapshot isolation is achieved by atomic reads at certain revisions
    - [x] Deterministic txn ~~executor~~ mvcc.Engine
    - ~~[ ] Watch event log~~ Watches are outside of this project, as of yet
        - ~~refactor delete into emmiting phantom delete events, instead of noop if meta wasnt found in key_index~~
    - [x] Raft-triggered compaction
        - gonna be either periodic or retention window | See the combined compaction scheduler below
    - [x] DELETE -> tombstone marker
    - [x] Transactions
        - [x] applyTxnOp
            - distinguishing between txn ending errors and regural errors that should be converted into TxnOpResult
            - encode/decode should use binary so it doesnt return errors
        - [x] add TxnOpTypeGet = ~~"GET"~~ "RANGE" (if no writes chosen ops then then return early, no new rev needed)
        - [x] abort on read error
        - [x] kv http endpoint
    - ~~Compaction~~  (see combined compactor below)
        - automatic retention window: currentRev - compactedRev > THRESHOLD
        - deterministic
        - run from raft apply or by a fsm command directly
            - [ ] or let it be configurable: could be set to periodic (--compact-timer_hourly INT), could be window retention
        - simple :
            - at revision C:
            - for each key:
                - find all revisions
                - determine latest <= C
                - delete others
            - _meta/compacted_revision = C
        - advanced:
            - keep a separate (revision, key) -> nil bucket
            - lexicographically sotred for revision, way faster to scan and delete old versions for a key
        - or even better:
            - store _meta/compacted_revision
            - during compaction, only iterate keys whose latest rev < C
                - key_index: key -> metadata{modRev}
                - if modRev < C then: key is fully below compaction window AND all but latest can be pruned aggressively
        - production grade: incremental compaction
            - store _meta/compaction_cursor
            - process 10K entries or so
            - return and let raft do the rest of the commands
            - prevents fsm stalls, lateny spikes or leader blocking
    - [x] Snapshot
        - Storage layer already handles this
        - on restore, load _meta keys into RevisionManager
        - and also restore leases
    - [x] BUCKETS
        - [x] "kv/"
        - [x] "lease/"
        - [x] "_meta/"
            - current_revision
            - consistent_index
                - after apply log at raft log index i: store consistent_index = i
            - compacted_revision
        - ~~[x] "key_index/":~~
            - key index is gonna be inmemory, sort of like a cache
                so we dont have to store redundant data on disk, and dont have to do additional roundtrips
            - it stores all the revisions per key, handles deleted but revived keys via generations (list of revisions)
	        - ~~Latest metadata about each key. Stores key -> (createRevision uint64, modRevision uint64, version uint64, tombstone bool)~~
        - [x] ~~"key_history/"~~ "main/":
	        - Append only historical log of all version of a key. Stores (mainRev, subRev) -> Entry{key, value, createRev, modRev, version, tombstone, leaseID}
    - [x] Ops:
        - [x] GET:
            - Case 1: Read latest
                - lookup key_index
                - if tombstone == true then KeyNotFound
                - else fetch values from key_history using (key, modRev)
            Case 2: Read at revision R
                Scan key_history:
                    Find highest revision ≤ R.
                    if none then key not found 
                    else if found and tombstone then deleted at that point
                    else return keyvalue
        - [x] SET:
            - allocate new revision
            - insert into key_history (key, newRev) -> "foo"
            - update key_index (createRev == null ? newRew : createRev, modRev = newRev, version == null ? 1 : ++version, tombstone = false)
            - update _meta/current_revision = newRev
            - [ ] etcd starts a new "generation" after SET on a previously deleted value
        - [x] DEL:
            - allocate new revision
            - update key_history (key, newRev) -> tombstone_marker
            - update key_index (key) -> (modRev, tombstone = true, ++version)
            - update _meta/current_revision = newRev
    - [x] TODO: ~~snapshot  storage~~ compaction metrics too
    - [x] mvcc.writer: support revision.sub++ on txn ops
    - [x] lease:
        - [x] type Lease
        - [x] type LeaseManager
            - [x] impl
            - [x] test
        - [x] type LeaseHeap
            - [x] impl
            - [x] test
        - [x] type Checkpoint / CheckpointScheduler
            - [x] impl
            - [x] test
        - [x] type ExpiryLoop
            - [x] impl
            - [x] test
        - [x] lm.Restore()
            - [x] impl
            - [x] test
    - [x] background routines
        - [x] create an interface with OnLeadershipGranted(f) bool or have a channel that returns leadership grants/revocatinos
            and set ticker to nil if granted := <- C; granted == false or to the real one if granted == true
    - [x] fix kvservice tests with mocked raft or similar
    - [x] combined compaction scheduler
        - [x] periodic ticks, candidateRev = rev at last tick
        - [x] threshold under which we shouldnt compact
        - [x] BUT if were between periods, but writes have accumulated fast -> lets compact
        - [x] use util.Ticker
        - [x] instead of calling compactable.Compact(), propose a command.CompactCmd to the fsm and let fsm.store handle it
    
    - MARCH.21:
    - [x] read path
        - [x] eventual consistency: only leader 
        - [x] strong consistency: only leader + VerifyLeader()
    - [x] put request
        - [x] IgnoreLease
        - [x] IgnoreValue
    - [x] txn through kvservice (+ http)
        - [x] kvservice
        - [x] http
        - [x] general tests
        - [x] comparison panics: imporve cmp.Check()
            - gob treats var x *int = &0 as nil, so if you wanted to compare against value==0, it wouldve been value==DEREFERENCED_NIL_POINTER
            - so just use int64 instead of int64, mem layout is the same, and we already have the field target as the "discrimnator tag"
    - [x] backend.Defragment
        - [x] bytestore impl's Defrag()
        - [x] lock the backend and call store.Defrag()
            so no user sees an empty db while reading/writing
    - [x] KVStore.Restore() doesnt distinguish tombstones from regular entries:
        - After a snapshot restore, any key that was deleted will have its tombstone entry processed as a Put, making deleted keys reappear as live in the index
    - [ ] metrics
        - [x] raft.Stats() is constant in RaftMetrics!!!!
            - moved raft.Stats() call inside every GaugeFunc
        - [x] WriteTx Commit Observer
        - [ ] Snapshot metrics too
        - [ ] meaningful grafana dashboards
        - [x] figure out where and how to collect every kind of metric
            - [x] raft
            - [x] kv
            - ~~[ ] backend~~
            - [x] lease
    - [x] use time.Tick() instead of time.Ticker() (no need for ticker.Stop() from now on)
    - [ ] http layer touch ups
        - [ ] tls
            - [ ] tls on http server
            - [ ] tls on raft inter node communication
        - [x] handle http redirects from follower to leader
            - single host reverse proxy?
        - [x] http tests
            - [x] middleware
                - [x] readMiddleware
                    - switch request.Serializable 
                        - case True: serve reads localy
                        - case False: proxy to leader + VerfiyLeader() (later use ReadIndex optimization discussed in the raft paper)
                - [x] writeModeMiddleware
                    - proxy to leader
            - [x] kv
            - [x] lease
            - [x] ot
            - [x] health/debug
        - [x] response code
            - [x] only propose err should return 500 (503 to be exact)
            - [x] rest are 400
    - [ ] BatchingFSM
    - [ ] kv index
        - [ ] LICENSE from etcd: http://www.apache.org/licenses/LICENSE-2.0
        - [ ] batch kvindex updates, rollback on commit failure
    - [x] OT
        - [x] type TokenCodec
            - [x] impl
            - [x] test
        - [x] type OTManager
            - [x] impl
            - [x] test
        - [x] type OTService
            - [x] impl
            - [x] test
    - [x] watch API
        - [x] watcher
            - store key, end, startRev, currentRev and a buffered channel of events for send/sendAll
            - since channel is buffered, send/sendAll returns errWatcherOverloaded,
                we can try again later
        - [x] WatchHub: manages all watchers on the node, and their "synced" status
            - [x] CreateWatcher / CancelWatcher
            - [x] manage "up-to-date" watchers and "unsynced" watchers
                - synced or up-to-date if watcher.currentRev >= STORE_CURRENT_REV, unsynced otherwise
            - [x] OnCommit hooked into fsm.Apply, to process each new entry/event 
        - [x] UnsyncedLoop (background routine wrapping the WatchHub)
            - [x] tracks unsynced watchers, periodically feeds a subset of them events
                so they might eventually promote to synced watchers in the hub
            - [x] if a watcher fails to promote X times, it gets dropped
            - [x] promotion happens when the watchers.currentRev >= HIGHEST_REV_FOR_KEYRANGE, where KEYRANGE is [watcher.keyStart, watcher.keyEnd)
            - [x] to reduce latency in the fsm.Apply path (WatchHub.OnCommit()), batch promotion/deletion at the end of each tick, instead of locking the hub for the entire tick
            - [x] remove Reader.RevisionRange, use kvIndex.RevisionsRange(key, end []byte, startRev, endRev int64)
        - [x] Stream: creates/cancels watchers, runs them in go routines and collects messages through a public read only channle 
            - [x]manages its own subset of watcher, wraps the hub
            - [x] starts a collector go routine for each new watch created, and collects it
                in an output channel, emitting StreamEvents {watcher_id, event}
        - [x] Session: wraps a websocket connection, creates a Stream and handles ws reads/writes
            - [x] collector routine: collects events and messages then writes the json to the client
                - collects events from the stream
                - collects control messages from the session
                - wraps these in ServerMessage and writes them to the connection
                - writes have a timeout, that will bring down the whole session if writes are too slow
            - [x] dispatcher routine: reads client events and forwards them to the stream/ and their result to the collector for writes
                - dispatcher blocks on reading from the connection
                - one a client message arrives for watch create/cancel, it forwards the request to the stream, then sends the result to the collector go routine 
                - wraps these in ServerMessage and writes them to the collector
            - [x] fix _err = json.Marshal() on ServerMessage

    - APR. 11:
    - [x] cluster integration tests
    - [ ] live workload + web ui for stats, metrics and manual commands
    - [ ] BatchingFSM
    - [ ] kv index
        - [ ] LICENSE from etcd: http://www.apache.org/licenses/LICENSE-2.0
        - [ ] batch kvindex updates, rollback on commit failure

    APR. 20:
    - [ ] TODO: ADD RESPONSE_HEADER TO LEASE RESPONSES
    - [ ] ui:
        - [ ] kave client
        - [x] ot client
        - [ ] sections
            - [ ] idk
    

# CHORES
- [ ] use require in every test insteaf of if err != nil ...
- [ ] use either english or hungarian in all of the doc comments ???