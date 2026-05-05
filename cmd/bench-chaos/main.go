package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/balits/kave/internal/benchmarks"
)

var (
	kubeconfig = flag.String("kubeconfig", "", "path to kubeconfig")
	namespace  = flag.String("namespace", "kave", "kubernetes namespace")
	targetURL  = flag.String("target", "http://localhost:8000", "base URL of kave HTTP server")
	rps        = flag.Int("rps", 200, "write requests per second during chaos test")
	duration   = flag.Duration("duration", 30*time.Second, "total test duration")
)

func main() {
	flag.Parse()

	if *kubeconfig == "" {
		fmt.Println("error: --kubeconfig is required")
		flag.Usage()
		return
	}

	cfg := benchmarks.ChaosConfig{
		Kubeconfig: *kubeconfig,
		Namespace:  *namespace,
		TargetURL:  *targetURL,
		RPS:        *rps,
		Duration:   *duration,
	}

	fmt.Println("=== kave chaos test ===")
	fmt.Printf("Target:    %s\n", *targetURL)
	fmt.Printf("RPS:       %d\n", *rps)
	fmt.Printf("Duration:  %v\n", *duration)
	fmt.Println()

	result, err := benchmarks.RunChaosTest(cfg)
	if err != nil {
		fmt.Printf("error: %v\n", err)
	}

	_ = result
}
