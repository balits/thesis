package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/balits/kave/internal/benchmarks"
)

var (
	targetURL = flag.String("target", "http://localhost:8000", "base URL of kave HTTP server")
	duration  = flag.Duration("duration", 20*time.Second, "duration per phase")
)

var stressPhases = []benchmarks.Phase{
	{Label: "  50 RPS", Duration: 0, RPS: 50, WriteRatio: 1.0},
	{Label: "  100 RPS", Duration: 0, RPS: 100, WriteRatio: 1.0},
	{Label: "  200 RPS", Duration: 0, RPS: 200, WriteRatio: 1.0},
}

var mixedPhases = []benchmarks.Phase{
	{Label: "  200 RPS", Duration: 0, RPS: 200, WriteRatio: 0.3},
	{Label: "  500 RPS", Duration: 0, RPS: 500, WriteRatio: 0.3},
}

var readPhases = []benchmarks.Phase{
	{Label: "  200 RPS", Duration: 0, RPS: 200, WriteRatio: 0.0},
	{Label: "  500 RPS", Duration: 0, RPS: 500, WriteRatio: 0.0},
}

func main() {
	flag.Parse()

	client := benchmarks.NewKVClient(*targetURL)

	fmt.Println("=== kave benchmark suite ===")
	fmt.Println("Target:", *targetURL)
	fmt.Println("Duration/phase:", *duration)
	fmt.Println()

	for i := range stressPhases {
		stressPhases[i].Duration = *duration
	}
	for i := range mixedPhases {
		mixedPhases[i].Duration = *duration
	}
	for i := range readPhases {
		readPhases[i].Duration = *duration
	}

	fmt.Println("Phase 1: Write-heavy (100% writes)")
	for _, p := range stressPhases {
		benchmarks.RunPhase(client, p)
	}

	fmt.Println()
	fmt.Println("Phase 2: Mixed load (30% writes / 70% reads)")
	for _, p := range mixedPhases {
		benchmarks.RunPhase(client, p)
	}

	fmt.Println()
	fmt.Println("Phase 3: Read-heavy (0% writes / 100% reads)")
	for _, p := range readPhases {
		benchmarks.RunPhase(client, p)
	}

	fmt.Println()
	fmt.Println("=== done ===")
}
