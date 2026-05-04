package benchmarks

import (
	"context"
	"fmt"
	"os/exec"
	"time"
)

type ChaosResult struct {
	Downtime        time.Duration
	LeaderElection  time.Duration
	ErrorsDuringGap int
	TotalDuration   time.Duration
}

type ChaosConfig struct {
	Kubeconfig  string
	Namespace   string
	StatefulSet string
	TargetURL   string
	RPS         int
	Duration    time.Duration
}

func RunChaosTest(cfg ChaosConfig) (*ChaosResult, error) {
	result := &ChaosResult{}
	start := time.Now()

	client := NewKVClient(cfg.TargetURL)

	errors := 0
	var firstError time.Time
	var recoveryTime time.Time
	recovered := false

	loadDone := make(chan struct{})
	go func() {
		defer close(loadDone)
		interval := time.Second / time.Duration(cfg.RPS)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		timeout := time.After(cfg.Duration)
		for {
			select {
			case <-timeout:
				return
			case <-ticker.C:
				key := fmt.Sprintf("chaos-%d", time.Now().UnixNano()%1000000)
				d, ok := client.Put(key, []byte("chaos-test"))
				_ = d
				if !ok {
					if !recovered && firstError.IsZero() {
						firstError = time.Now()
					}
					errors++
				}
				if recovered {
					continue
				}
				if ok && !firstError.IsZero() && recoveryTime.IsZero() {
					recoveryTime = time.Now()
					recovered = true
				}
			}
		}
	}()

	time.Sleep(2 * time.Second)

	fmt.Println(">> killing leader pod...")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	killCmd := exec.CommandContext(ctx, "kubectl", "delete", "pod",
		"-l", "kave/role=voter,app.kubernetes.io/name=kave",
		"--field-selector", "metadata.name=kave-voter-0",
		"-n", cfg.Namespace,
		"--kubeconfig", cfg.Kubeconfig,
	)
	killCmd.Stdout = nil
	killCmd.Stderr = nil
	if err := killCmd.Run(); err != nil {
		return nil, fmt.Errorf("failed to kill leader pod: %w", err)
	}
	fmt.Println(">> leader pod deleted")

	<-loadDone

	result.TotalDuration = time.Since(start)
	result.ErrorsDuringGap = errors

	if !firstError.IsZero() && !recoveryTime.IsZero() {
		result.Downtime = recoveryTime.Sub(firstError)
	}

	fmt.Printf("\n=== Chaos Test Results ===\n")
	fmt.Printf("  Total duration:   %v\n", result.TotalDuration.Round(time.Millisecond))
	fmt.Printf("  Errors during gap: %d\n", result.ErrorsDuringGap)
	if result.Downtime > 0 {
		fmt.Printf("  Downtime:         %v\n", result.Downtime.Round(time.Millisecond))
	} else {
		fmt.Printf("  Downtime:         no downtime observed\n")
	}

	return result, nil
}
