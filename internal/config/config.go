package config

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/balits/kave/internal/compaction"
	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/mtls"
	"github.com/balits/kave/internal/ot"
	"github.com/balits/kave/internal/peer"
	"github.com/balits/kave/internal/schema"
	"github.com/balits/kave/internal/storage"
	"github.com/balits/kave/internal/transport/http"
	"github.com/balits/kave/internal/util/logutil"
	"github.com/hashicorp/raft"
)

type Config struct {
	Bootstrap      bool
	Me             peer.Peer
	PodNamespace   string
	RaftCfg        *raft.Config
	AdminAuthToken string

	*ConfigJson
}

type ConfigJson struct {
	LoggerOptions              logutil.Options       `json:"logger"`
	KvOptions                  kv.Options            `json:"kv"`
	PeerDiscoveryOptions       peer.DiscoveryOptions `json:"peer_discovery"`
	StorageOpts                storage.Options       `json:"storage"`
	CompactionOpts             compaction.Options    `json:"compaction"`
	OtOpts                     ot.Options            `json:"ot"`
	CheckpointIntervalMinutes  time.Duration         `json:"checkpoint_interval_minutes"`
	RatelimiterOpts            http.RatelimitOptions `json:"ratelimiter"`
	MtlsOptions                mtls.Options          `json:"mtls"`
	RaftOpts                   RaftOpts              `json:"raft"`
	ApplyLagReadinessThreshold uint                  `json:"apply_lag_readiness_threshold"`
	ApplyLagThreshold          uint                  `json:"apply_lag_threshold"`
}

type RaftOpts struct {
	SnapshotIntervalSec int `json:"snapshot_interval_sec"`
	SnapshotThreshold   int `json:"snapshot_threshold"`
	TrailingLogs        int `json:"trailing_logs"`
	MaxAppendEntries    int `json:"max_append_entries"`
}

func (cj *ConfigJson) check() error {
	if err := cj.StorageOpts.Check(); err != nil {
		return err
	}
	if err := cj.CompactionOpts.Check(); err != nil {
		return err
	}
	if err := cj.OtOpts.Check(); err != nil {
		return err
	}
	if err := cj.PeerDiscoveryOptions.Check(); err != nil {
		return err
	}
	if err := cj.KvOptions.Check(); err != nil {
		return err
	}
	if err := cj.LoggerOptions.Check(); err != nil {
		return err
	}
	if err := cj.MtlsOptions.Check(); err != nil {
		return err
	}

	return nil
}

// ToConfig parses the raw json configuration, and turns them
// into a usable Config, or returns with an error.
// It also removes a given node from the peer list
func (cj *ConfigJson) ToConfig() (*Config, error) {
	if err := cj.check(); err != nil {
		return nil, fmt.Errorf("failed to validate config json: %v", err)
	}

	c := &Config{
		ConfigJson: cj,
	}
	c.StorageOpts.InitialBuckets = schema.AllBuckets

	return c, nil
}

func LoadConfig() *Config {
	fs := flag.NewFlagSet("kave", flag.ExitOnError)

	// TODO: refactor env vars into config.env.<env-var>?
	var (
		configPath   = fs.String("config", "", "path to config file (required)")
		nodeID       = fs.String("node_id", os.Getenv("POD_NAME"), "id of the raft node")
		podNamespace = fs.String("namespace", os.Getenv("POD_NAMESPACE"), "namespace of the pod, when using k8s")
		podFqdn      = fs.String("fqdn", os.Getenv("POD_FQDN"), "fully qualified domain name of the pod, when using k8s")
		raftPort     = fs.String("raft_port", os.Getenv("RAFT_PORT"), "raft port of the raft node")
		httpPort     = fs.String("http_port", os.Getenv("HTTP_PORT"), "http port of the raft node")
		// role       = fs.String("role", os.Getenv("NODE_ROLE"), "role of the node (voter | learner)")
		adminAuthToken = fs.String("admin_token", os.Getenv("ADMIN_TOKEN"), "super secret token for admin routes")
	)

	err := fs.Parse(os.Args[1:])
	check("parsing flagset", err)

	checkRequiredField("config", configPath)
	checkRequiredField("node_id", configPath)
	checkRequiredField("raft_port", raftPort)
	checkRequiredField("http_port", httpPort)
	checkRequiredField("admin_token", adminAuthToken)

	me := peer.Peer{
		NodeID:   *nodeID,
		RaftPort: *raftPort,
		HttpPort: *httpPort,
		Hostname: optionalString(podFqdn),
	}
	check("peer info about me", me.Check())

	var cj ConfigJson
	file, err := os.Open(*configPath)
	check("config file", err)

	d := json.NewDecoder(file)

	check("config file: decoding failed:", d.Decode(&cj))

	cfg, err := cj.ToConfig()
	check("config file: converting json to config object", err)

	cfg.Bootstrap = strings.HasSuffix(*nodeID, "-0")
	cfg.Me = me
	cfg.PodNamespace = optionalString(podNamespace)
	cfg.RaftCfg = NewDefaultRaftConfig(cfg.Me.NodeID, &cfg.RaftOpts)
	cfg.AdminAuthToken = *adminAuthToken

	// debug certain fields
	fmt.Printf(
		"node_id: %s\n"+
			"\tloaded cert file: %s\n"+
			"\tloaded key file: %s\n"+
			"\tloaded ca file: %s\n",
		cfg.Me.NodeID,
		cfg.MtlsOptions.CertFile,
		cfg.MtlsOptions.KeyFile,
		cfg.MtlsOptions.CaFile,
	)

	return cfg
}

func optionalString(s *string) string {
	if s == nil {
		return ""
	} else {
		return *s
	}
}

func emptyString(field string, val *string) error {
	if val == nil || len(*val) == 0 {
		return fmt.Errorf("field %s is empty", field)
	}
	return nil
}

func checkRequiredField(field string, val *string) {
	check("expected value for field, got nil/empty: ", emptyString(field, val))
}

func check(msg string, err error) {
	if err != nil {
		panic(fmt.Errorf("fatal error: %s: %v", msg, err))
	}
}
