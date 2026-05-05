package config

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/balits/kave/internal/mtls"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

type RaftDependencies struct {
	LogStore      raft.LogStore
	SnapshotStore raft.SnapshotStore
	StableStore   raft.StableStore
	Transport     raft.Transport
	HcLogger      hclog.Logger
}

func NewDefaultRaftConfig(nodeID string, opts *RaftOpts) *raft.Config {
	var o RaftOpts
	if opts != nil {
		o = *opts
	}
	if o.SnapshotIntervalSec <= 0 {
		o.SnapshotIntervalSec = 300
	}
	if o.SnapshotThreshold <= 0 {
		o.SnapshotThreshold = 16384
	}
	if o.TrailingLogs <= 0 {
		o.TrailingLogs = 10240
	}
	if o.MaxAppendEntries <= 0 {
		o.MaxAppendEntries = 128
	}

	return &raft.Config{
		LocalID:            raft.ServerID(nodeID),
		ProtocolVersion:    raft.ProtocolVersionMax,
		HeartbeatTimeout:   2500 * time.Millisecond,
		ElectionTimeout:    5000 * time.Millisecond,
		CommitTimeout:      100 * time.Millisecond,
		MaxAppendEntries:   o.MaxAppendEntries,
		ShutdownOnRemove:   true,
		TrailingLogs:       uint64(o.TrailingLogs),
		SnapshotInterval:   time.Duration(o.SnapshotIntervalSec) * time.Second,
		SnapshotThreshold:  uint64(o.SnapshotThreshold),
		LeaderLeaseTimeout: 1000 * time.Millisecond,
		LogLevel:           "DEBUG",
	}
}

func NewRaftDependencies(cfg *Config) (*RaftDependencies, error) {
	var (
		logs      raft.LogStore
		stable    raft.StableStore
		snapshot  raft.SnapshotStore
		transport raft.Transport
		err       error

		listenAddr    = cfg.Me.GetRaftListenAddress()
		advertiseAddr = cfg.Me.GetRaftAddress()
	)

	if testing.Testing() {
		logs = raft.NewInmemStore()
		stable = raft.NewInmemStore()
		snapshot = raft.NewInmemSnapshotStore()
		_, transport = raft.NewInmemTransport(advertiseAddr)
	} else {
		logstorePath := filepath.Join(cfg.StorageOpts.Dir, "logstore.db")
		logs, err = raftboltdb.NewBoltStore(logstorePath)
		if err != nil {
			return nil, fmt.Errorf("failed to create logstore: %v", err)
		}

		stablePath := filepath.Join(cfg.StorageOpts.Dir, "stablestore.db")
		stable, err = raftboltdb.NewBoltStore(stablePath)
		if err != nil {
			return nil, fmt.Errorf("couldn't create raft stable store: %w", err)
		}

		snapshot, err = raft.NewFileSnapshotStoreWithLogger(cfg.StorageOpts.Dir, 10, cfg.RaftCfg.Logger)
		if err != nil {
			return nil, fmt.Errorf("couldn't create raft snapshot store: %w", err)
		}

		streamLayer, err := mtls.NewStreamLayer(listenAddr, advertiseAddr, cfg.MtlsOptions.CertFile, cfg.MtlsOptions.KeyFile, cfg.MtlsOptions.CaFile)
		if err != nil {
			return nil, fmt.Errorf("failed to create stream layer (listen = %s, adverstise = %s): %v", listenAddr, advertiseAddr, err)
		}

		transport = raft.NewNetworkTransportWithConfig(&raft.NetworkTransportConfig{
			Stream:  streamLayer,
			MaxPool: 3,
			Timeout: 10 * time.Second,
			Logger:  cfg.RaftCfg.Logger,
		})
	}

	deps := &RaftDependencies{
		LogStore:      logs,
		StableStore:   stable,
		SnapshotStore: snapshot,
		Transport:     transport,
		HcLogger:      raft.DefaultConfig().Logger,
	}

	return deps, nil
}
