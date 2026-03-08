package benchmark

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"benchmark/internal/db"
	"benchmark/internal/metrics"
)

func RunRead(db db.Database, requests int, maxOrder int64, table string) {

	var durations []time.Duration
	durations = make([]time.Duration, 0, requests)

	var done atomic.Int64
	stop := make(chan struct{})

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				fmt.Printf("\rREAD Progress: %d/%d", done.Load(), requests)
			case <-stop:
				return
			}
		}
	}()

	for i := 0; i < requests; i++ {

		id := rand.Int63n(maxOrder) + 1

		start := time.Now()

		err := db.GetOrder(id, table)

		if err != nil {
			fmt.Println("\nGetOrder error:", err)
			continue
		}

		durations = append(durations, time.Since(start))
		done.Add(1)
	}

	close(stop)

	fmt.Printf("\rREAD Progress: %d/%d\n", requests, requests)

	res := metrics.Calculate(durations)

	fmt.Println("READ BENCHMARK")
	fmt.Println("Avg:", res.Avg)
	fmt.Println("P95:", res.P95)
	fmt.Println("Min:", res.Min)
	fmt.Println("Max:", res.Max)
}