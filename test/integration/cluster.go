package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/compaction"
	"github.com/balits/kave/internal/config"
	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/node"
	"github.com/balits/kave/internal/ot"
	"github.com/balits/kave/internal/peer"
	"github.com/balits/kave/internal/schema"
	"github.com/balits/kave/internal/storage"
	"github.com/balits/kave/internal/transport"
	_http "github.com/balits/kave/internal/transport/http"
	"github.com/balits/kave/internal/util/logutil"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

// testAdminToken is the shared admin token used by all test clusters.
// It must match the AdminAuthToken set in defaultNodeConfig().
const testAdminToken = "test-admin-token"

type cluster struct {
	tb           testing.TB
	n            int
	nodes        []*node.Node
	cfgs         []*config.Config
	nodeContexts []context.Context
	cancels      []context.CancelFunc
	rootLogger   *slog.Logger
	peerList     []peer.Peer
	ctx          context.Context
	cancel       context.CancelFunc
}

func newClusterWithConfig(tb testing.TB, n int, nodeConfig *config.Config, raftConfig *raft.Config) *cluster {
	c := _newcluster(tb, n, nodeConfig, raftConfig)
	c.run()
	return c
}

// newCluster creates a cluster with n nodes, and an optional raftConfig, usefull
// for setting custom options like SnapshotInterval or SnapshotThreshold.
// The localID (nodeID) field on the raft config will always be set the the given nodes ID
func newCluster(tb testing.TB, n int) *cluster {
	c := _newcluster(tb, n, nil, nil)
	c.run()
	return c
}

func _newcluster(tb testing.TB, n int, nodeConfig *config.Config, raftConfig *raft.Config) *cluster {
	if n != 1 && n != 3 && n != 5 {
		tb.Fatalf("expected n to be 1, 3, 5, got %d", n)
	}

	var loglevel slog.Level
	if testing.Verbose() {
		loglevel = slog.LevelDebug
	} else {
		loglevel = slog.LevelError
	}
	logger := logutil.NewTestLogger(tb, loglevel)

	ctx, cancel := context.WithCancel(tb.Context())
	c := &cluster{
		tb:           tb,
		n:            n,
		nodes:        make([]*node.Node, n),
		cfgs:         make([]*config.Config, n),
		cancels:      make([]context.CancelFunc, n),
		nodeContexts: make([]context.Context, n),
		ctx:          ctx,
		cancel:       cancel,
		rootLogger:   logger,
	}

	for i := range c.n {
		raftPort := randomPort(tb)
		httpPort := randomPort(tb)
		c.peerList = append(c.peerList, peer.Peer{
			NodeID:   fmt.Sprintf("testnode-%d", i),
			Hostname: "127.0.0.1",
			RaftPort: raftPort,
			HttpPort: httpPort,
		})
	}

	var peerStr strings.Builder
	for i, p := range c.peerList {
		peerStr.WriteString(p.String())
		if i < c.n-1 {
			peerStr.WriteString(",")
		}
	}
	peers := peerStr.String()

	for i := range c.n {
		me := c.peerList[i]
		c.cfgs[i] = testConfig(me, peers, nodeConfig, raftConfig)
		c.createNode(i)
	}

	return c
}

func (c *cluster) run() {
	c.connectNetworks()
	servers := make([]raft.Server, c.n)

	for i, n := range c.nodes {
		servers[i] = raft.Server{
			ID:       raft.ServerID(n.Me.NodeID),
			Address:  n.Me.GetRaftAddress(),
			Suffrage: raft.Voter,
		}
	}
	cfg := raft.Configuration{Servers: servers}

	for _, n := range c.nodes {
		// testnode-0 wins, rest see existing state
		err := n.Raft.BootstrapCluster(cfg).Error()
		if err != nil {
			c.tb.Fatalf("failed to run cluster: failed to bootstrap cluster %v", err)
		}
	}

	for i, n := range c.nodes {
		go func(ctx context.Context) {
			_ = n.Run(ctx)
		}(c.nodeContexts[i])
	}
	c.waitClusterReady(10 * time.Second)

	leader, _ := c.waitLeader(5 * time.Second)
	_, err := leader.ProposeFunc(c.ctx, command.Command{
		Kind: command.KindOTGenerateClusterKey,
		OTGenerateClusterKey: &command.CmdOTGenerateClusterKey{
			Key: ot.RandomKey256(),
		},
	})
	require.NoError(c.tb, err)

	time.Sleep(1000 * time.Millisecond) // wait for replication
}

func (c *cluster) waitLeader(timeout time.Duration) (*node.Node, int) {
	return c.WaitState(raft.Leader, timeout)
}

func (c *cluster) WaitState(state raft.RaftState, timeout time.Duration) (*node.Node, int) {
	var n *node.Node
	var idx int
	require.Eventually(c.tb, func() bool {
		for i, node := range c.nodes {
			if node != nil && node.Raft != nil && node.Raft.State() == state {
				n = node
				idx = i
				return true
			}
		}
		return false
	}, timeout, timeout/10, "cluster failed to reach state %s", state)
	return n, idx
}

func (c *cluster) WaitKillLeader(i int, timeout time.Duration) {
	fmt.Printf("\n\nkilling leader\n\n")
	if c.nodes[i] == nil || c.nodes[i].Raft == nil {
		c.tb.Fatalf("node %d is not running", i)
	}
	c.cancels[i]()
	l1 := c.nodes[i]
	require.Eventually(c.tb, func() bool {
		if l1 != nil && l1.Raft != nil && l1.Raft.State() != raft.Leader {
			return true
		}
		return false
	}, timeout, timeout/20, "leader failed to step downs")

	l2, _ := c.WaitState(raft.Leader, 10*time.Second)
	require.NotNil(c.tb, l2)
	require.NotEqual(c.tb, l1.Me.NodeID, l2.Me.NodeID, "A new node should have been elected leader")
}

func (c *cluster) waitClusterReady(timeout time.Duration) {
	var leaderIdx int
	condQuorum := func() bool {
		leaders, followers := 0, 0
		for i, n := range c.nodes {
			if n.Raft.State() == raft.Leader {
				leaderIdx = i
				leaders++
			} else if n.Raft.State() == raft.Follower {
				followers++
			}
		}
		if leaders != 1 {
			return false
		}
		if followers != c.n-1 {
			return false
		}

		for i := range c.n {
			if s, err := c.tryReady(i); err != nil || s != http.StatusOK {
				return false
			}
		}

		return true
	}

	l := c.nodes[leaderIdx]
	condPeersJoined := func() bool {
		f := l.Raft.GetConfiguration()
		if err := f.Error(); err != nil {
			return false
		}
		return len(f.Configuration().Servers) == c.n
	}

	require.Eventually(c.tb, condQuorum, timeout, timeout/10, "cluster failed to form quorum under %s", timeout)
	require.Eventually(c.tb, condPeersJoined, timeout, time.Millisecond/10, "not all peers joined the raft cluster configuration")

	c.tb.Log("\n\n==============\nCLUSTER READY\n==============\n\n")
}

func (c *cluster) ForceSnapshot(i int) error {
	if c.nodes[i] == nil || c.nodes[i].Raft == nil {
		return fmt.Errorf("node %d is not running", i)
	}
	return c.nodes[i].Raft.Snapshot().Error()
}

func (c *cluster) teardown() {
	c.cancel()
	require.Eventually(c.tb, func() bool {
		for _, n := range c.nodes {
			if !n.IsShutdown {
				return false
			}
		}

		return true
	}, 5*time.Second, time.Millisecond*200, "failed to teardown cluster: not all nodes are shutdown")

	time.Sleep(5 * time.Second)
}

func (c *cluster) connectNetworks() {
	for i := range c.nodes {
		c.reconnectNode(i)
	}
}

func (c *cluster) reconnectNode(i int) {
	a := c.nodes[i]
	if a == nil || a.RaftDeps == nil {
		return
	}
	trA, ok := a.RaftDeps.Transport.(*raft.InmemTransport)
	if !ok {
		c.tb.Fatal("node was not using inmem transport")
	}

	for j, b := range c.nodes {
		if i == j || b == nil || b.RaftDeps == nil {
			continue
		}
		trB, ok := b.RaftDeps.Transport.(*raft.InmemTransport)
		if !ok {
			continue
		}
		trA.Connect(a.Me.GetRaftAddress(), trB)
		trB.Connect(a.Me.GetRaftAddress(), trA)
	}
}

func testConfig(me peer.Peer, peers string, nodeConfig *config.Config, raftCfg *raft.Config) (outConfig *config.Config) {
	bootstrap := strings.HasSuffix(me.NodeID, "-0")

	var _raftCfg *raft.Config
	if raftCfg == nil {
		// tweaked config optimized for tests
		_raftCfg = config.NewDefaultRaftConfig(me.NodeID, nil)
		_raftCfg.ElectionTimeout = 500 * time.Millisecond
		_raftCfg.HeartbeatTimeout = 100 * time.Millisecond
		_raftCfg.LeaderLeaseTimeout = 50 * time.Millisecond
		_raftCfg.SnapshotInterval = time.Second * 5
		_raftCfg.SnapshotThreshold = 5
	} else {
		_raftCfg = raftCfg
	}
	_raftCfg.LocalID = raft.ServerID(me.NodeID)

	if nodeConfig == nil {
		outConfig = defaultNodeConfig()
	} else {
		outConfig = nodeConfig
	}

	outConfig.Bootstrap = bootstrap
	outConfig.Me = me
	outConfig.RaftCfg = _raftCfg
	outConfig.PeerDiscoveryOptions.Peers = &peers

	return outConfig
}

func (c *cluster) createNode(i int) {
	cfg := c.cfgs[i]
	nodeCtx, cancel := context.WithCancel(c.ctx)
	c.cancels[i] = cancel
	c.nodeContexts[i] = nodeCtx
	n, err := node.New(cfg, c.rootLogger, metrics.InitTestPrometheus())
	require.NoError(c.tb, err, "failed to create node")
	c.nodes[i] = n
}

func (c *cluster) restartNode(i int) {
	nodeCtx, cancel := context.WithCancel(c.ctx)
	c.cancels[i] = cancel

	c.cfgs[i].Bootstrap = false
	n, err := node.New(c.cfgs[i], c.rootLogger, metrics.InitTestPrometheus())
	require.NoError(c.tb, err, "failed to create node")
	c.nodes[i] = n

	c.reconnectNode(i)

	go func() {
		_ = n.Run(nodeCtx)
	}()
}

func randomPort(tb testing.TB) string {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		tb.Fatal("failed to generate random port", err)
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		tb.Fatal("failed to generate random port", err)
	}

	defer func() {
		_ = l.Close()
	}()

	return fmt.Sprintf("%d", (l.Addr().(*net.TCPAddr).Port))
}

func do(tb testing.TB, node *node.Node, method string, route string, reqBody any, respBody any) int {
	return doWithHeaders(tb, node, method, route, reqBody, respBody, nil)
}

func doAdmin(tb testing.TB, node *node.Node, method string, route string, reqBody any, respBody any) int {
	return doWithHeaders(tb, node, method, route, reqBody, respBody, map[string]string{
		transport.AdminAuthTokenHeaderName: testAdminToken,
	})
}

func doWithHeaders(tb testing.TB, node *node.Node, method string, route string, reqBody any, respBody any, headers map[string]string) int {
	tb.Helper()
	var bodyReader io.Reader
	if reqBody != nil {
		b, err := json.Marshal(reqBody)
		require.NoError(tb, err)
		bodyReader = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, node.Me.HttpURL()+route, bodyReader)
	require.NoError(tb, err)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(tb, err)
	defer func() {
		_ = resp.Body.Close()
	}()

	if respBody != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		err = json.NewDecoder(resp.Body).Decode(respBody)
		require.NoError(tb, err, "failed to decode response body")
	}
	return resp.StatusCode
}

func (c *cluster) requireReady(i int) int {
	if i < 0 || i >= len(c.nodes) {
		c.tb.Fatalf("tried to access node %d, lenght is %d", i, len(c.nodes))
	}

	url := c.nodes[i].Me.HttpURL()
	resp, err := http.Get(url + "/readyz")
	require.NoError(c.tb, err)
	defer func() {
		_ = resp.Body.Close()
	}()
	return resp.StatusCode
}

func (c *cluster) tryReady(i int) (int, error) {
	if i < 0 || i >= len(c.nodes) {
		c.tb.Fatalf("tried to access node %d, lenght is %d", i, len(c.nodes))
	}

	url := c.nodes[i].Me.HttpURL()
	resp, err := http.Get(url + "/readyz")
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	return resp.StatusCode, nil
}

func (c *cluster) waitReady(i int) {
	require.Eventually(c.tb, func() bool {
		s, err := c.tryReady(i)
		if err != nil {
			return false
		}
		return s == http.StatusOK
	}, 15*time.Second, 500*time.Millisecond, "node failed to become ready")
}

func defaultNodeConfig() *config.Config {
	cj := &config.ConfigJson{
		LoggerOptions: logutil.Options{
			Kind:  logutil.LoggerKindText,
			Level: logutil.LevelDebug,
		},
		KvOptions: kv.Options{
			MaxKeySize:   256,
			MaxValueSize: 256,
		},
		PeerDiscoveryOptions: peer.DiscoveryOptions{
			Mode: peer.DiscoveryModeStatic,
		},
		StorageOpts: storage.Options{
			Kind:           storage.StorageKindInMemory,
			InitialBuckets: schema.AllBuckets,
		},
		CompactionOpts:            compaction.DefaultOptions,
		CheckpointIntervalMinutes: 2 * time.Second,
		// set to a gorbillion since were testing the FSM/DB not the ratelimiter
		RatelimiterOpts: _http.RatelimitOptions{
			Read:  _http.NewRateLimiterConfig(99999, 99999),
			Write: _http.NewRateLimiterConfig(99999, 99999),
		},
		OtOpts: ot.DefaultOptions,
	}
	return &config.Config{
		AdminAuthToken: testAdminToken,
		ConfigJson:     cj,
	}
}

func configWithoutCompactionScheduler() *config.Config {
	cfg := defaultNodeConfig()
	cfg.CompactionOpts.Threshold = 99999
	cfg.CompactionOpts.MaxRevGap = 99999
	cfg.CompactionOpts.IntervalMin = 99999
	return cfg
}

func raftConfigWithFastCompaction() *raft.Config {
	raftCfg := raft.DefaultConfig()
	raftCfg.SnapshotInterval = 2 * time.Second
	raftCfg.SnapshotThreshold = 5
	return raftCfg
}

func configWithRateLimitTest() *config.Config {
	cfg := defaultNodeConfig()
	cfg.RatelimiterOpts.Write.Rps = 5
	cfg.RatelimiterOpts.Write.Burst = 5
	return cfg
}
