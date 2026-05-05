package benchmarks

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var debug400Count int64

type Stats struct {
	mu        sync.Mutex
	latencies []time.Duration
	total     int64
	ok        int64
	fail      int64
}

func (s *Stats) Add(duration time.Duration, ok bool) {
	atomic.AddInt64(&s.total, 1)
	if ok {
		atomic.AddInt64(&s.ok, 1)
	} else {
		atomic.AddInt64(&s.fail, 1)
	}
	s.mu.Lock()
	s.latencies = append(s.latencies, duration)
	s.mu.Unlock()
}

func (s *Stats) Latencies() []time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]time.Duration, len(s.latencies))
	copy(out, s.latencies)
	return out
}

func Percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(float64(len(sorted))*p)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func (s *Stats) Print(label string, duration time.Duration) {
	latencies := s.Latencies()
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	avg := time.Duration(0)
	for _, l := range latencies {
		avg += l
	}
	if len(latencies) > 0 {
		avg = avg / time.Duration(len(latencies))
	}

	rps := float64(atomic.LoadInt64(&s.ok)) / duration.Seconds()

	fmt.Printf("  %-12s  rps=%6.0f  total=%d  ok=%d  fail=%d  avg=%v  p50=%v  p95=%v  p99=%v\n",
		label, rps, s.total, s.ok, s.fail,
		avg.Round(time.Microsecond),
		Percentile(latencies, 0.50).Round(time.Microsecond),
		Percentile(latencies, 0.95).Round(time.Microsecond),
		Percentile(latencies, 0.99).Round(time.Microsecond),
	)
}

type KVClient struct {
	client *http.Client
	base   string
	putURL string
	getURL string
}

func NewKVClient(baseURL string) *KVClient {
	return &KVClient{
		client: &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        200,
				MaxIdleConnsPerHost: 200,
			},
		},
		base:   baseURL,
		putURL: baseURL + "/v1/kv/put",
		getURL: baseURL + "/v1/kv/range",
	}
}

func (c *KVClient) Put(key string, value []byte) (time.Duration, bool) {
	body := map[string]interface{}{
		"key":   base64.StdEncoding.EncodeToString([]byte(key)),
		"value": base64.StdEncoding.EncodeToString(value),
	}
	data, _ := json.Marshal(body)

	start := time.Now()
	req, _ := http.NewRequest("POST", c.putURL, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	d := time.Since(start)

	if err != nil {
		fmt.Fprintf(os.Stderr, "PUT err: %v\n", err)
		return d, false
	}
	defer func() { _ = resp.Body.Close() }()
	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusBadRequest {
			c := atomic.AddInt64(&debug400Count, 1)
			if c <= 5 {
				fmt.Fprintf(os.Stderr, "PUT status: %d key=%s body=%s\n", resp.StatusCode, key, string(bodyBytes))
			}
		} else {
			fmt.Fprintf(os.Stderr, "PUT status: %d (key=%s)\n", resp.StatusCode, key)
		}
	}
	return d, resp.StatusCode == http.StatusOK
}

func (c *KVClient) Get(key string) (time.Duration, bool) {
	body := map[string]interface{}{
		"key":          base64.StdEncoding.EncodeToString([]byte(key)),
		"serializable": true,
	}
	data, _ := json.Marshal(body)

	start := time.Now()
	resp, err := c.client.Post(c.getURL, "application/json", bytes.NewReader(data))
	d := time.Since(start)

	if err != nil {
		return d, false
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return d, resp.StatusCode == http.StatusOK
}

type Phase struct {
	Label      string
	Duration   time.Duration
	RPS        int
	WriteRatio float64
}

func RunPhase(client *KVClient, phase Phase) *Stats {
	stats := &Stats{}
	stop := make(chan struct{})
	var wg sync.WaitGroup

	interval := time.Second / time.Duration(phase.RPS)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				wg.Add(1)
				go func() {
					defer wg.Done()

					key := fmt.Sprintf("bench-%d", time.Now().UnixNano()%1000000)
					value := make([]byte, 256)

					var d time.Duration
					var ok bool
					if float64(time.Now().UnixNano()%100)/100.0 < phase.WriteRatio {
						d, ok = client.Put(key, value)
					} else {
						d, ok = client.Get(key)
					}
					stats.Add(d, ok)
				}()
			}
		}
	}()

	time.Sleep(phase.Duration)
	close(stop)
	wg.Wait()
	stats.Print(phase.Label, phase.Duration)
	return stats
}
