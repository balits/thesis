package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type KVMetrics struct {
	KeyCount      prometheus.GaugeFunc
	WatchersCount prometheus.Gauge

	// derived from mvcc.KVStore
	CurrentRev   prometheus.GaugeFunc
	CompactedRev prometheus.GaugeFunc

	// errors per cmd
	ReadErrorsTotal   prometheus.Counter
	PutErrorsTotal    prometheus.Counter
	DeleteErrorsTotal prometheus.Counter
	CompactionErrors  prometheus.Counter

	// cmd counts
	ReadsTotal   prometheus.Counter
	PutsTotal    prometheus.Counter
	DeletesTotal prometheus.Counter

	CommitedWritesTotal prometheus.Counter // count of committed transactions
	CompactionsTotal    prometheus.Counter // count of compactions

	ReadDurationSec prometheus.Histogram
	// PutDurationSec    prometheus.Histogram
	// DeleteDurationSec prometheus.Histogram
	TxnDurationSec        prometheus.Histogram // lifetime of a mvcc.writer
	CompactionDurationSec prometheus.Histogram
}

func NewKVMetrics(
	reg prometheus.Registerer,
	currentRevFn gaugeFn,
	compactedRevFn gaugeFn,
	keyCountFn gaugeFn,
) *KVMetrics {
	factory := promauto.With(reg)

	return &KVMetrics{
		KeyCount: factory.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "kave",
			Subsystem: "kv",
			Name:      "key_count",
			Help:      "Current number of keys in the store.",
		}, keyCountFn),
		WatchersCount: factory.NewGauge(prometheus.GaugeOpts{
			Namespace: "kave",
			Subsystem: "kv",
			Name:      "watcher_count",
			Help:      "Current number of watchers in the store.",
		}),
		CurrentRev: factory.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "kave",
			Subsystem: "kv",
			Name:      "current_revision",
			Help:      "Current main revision of the store.",
		}, currentRevFn),
		CompactedRev: factory.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "kave",
			Subsystem: "kv",
			Name:      "compacted_revision",
			Help:      "Revision up to which the store has been compacted.",
		}, compactedRevFn),

		ReadErrorsTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "kave",
			Subsystem: "kv",
			Name:      "read_errors_total",
			Help:      "Total errors during Read operations.",
		}),
		PutErrorsTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "kave",
			Subsystem: "kv",
			Name:      "put_errors_total",
			Help:      "Total errors during PUT operations.",
		}),
		DeleteErrorsTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "kave",
			Subsystem: "kv",
			Name:      "delete_errors_total",
			Help:      "Total errors during DEL operations.",
		}),
		CompactionErrors: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "kave",
			Subsystem: "kv",
			Name:      "compaction_errors_total",
			Help:      "Total errors during compaction.",
		}),

		PutsTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "kave",
			Subsystem: "kv",
			Name:      "puts_total",
			Help:      "Total Put operations.",
		}),
		DeletesTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "kave",
			Subsystem: "kv",
			Name:      "deletes_total",
			Help:      "Total Delete operations.",
		}),
		ReadsTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "kave",
			Subsystem: "kv",
			Name:      "reads_total",
			Help:      "Total Read operations.",
		}),
		CommitedWritesTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "kave",
			Subsystem: "kv",
			Name:      "committed_writes_total",
			Help:      "Total commited writes operations (put | del | txn).",
		}),
		CompactionsTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "kave",
			Subsystem: "kv",
			Name:      "compactions_total",
			Help:      "Total compaction operations.",
		}),

		ReadDurationSec: factory.NewHistogram(prometheus.HistogramOpts{
			Namespace: "kave",
			Subsystem: "kv",
			Name:      "read_duration_seconds",
			Help:      "Latency of Read operations.",
			Buckets:   []float64{.0001, .0005, .001, .005, .01, .05, .1},
		}),
		TxnDurationSec: factory.NewHistogram(prometheus.HistogramOpts{
			Namespace: "kave",
			Subsystem: "kv",
			Name:      "txn_duration_seconds",
			Help:      "Lifetime of transactions from start to commit.",
			Buckets:   []float64{.0001, .0005, .001, .005, .01, .05, .1},
		}),
		CompactionDurationSec: factory.NewHistogram(prometheus.HistogramOpts{
			Namespace: "kave",
			Subsystem: "kv",
			Name:      "compaction_duration_seconds",
			Help:      "Latency of compaction operations.",
			Buckets:   []float64{.001, .005, .01, .05, .1, .5, 1},
		}),
	}
}
