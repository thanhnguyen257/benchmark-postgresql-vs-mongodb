package metrics

import (
	"sort"
	"time"
)

type Result struct {
	Avg time.Duration
	P95 time.Duration
	Min time.Duration
	Max time.Duration
}

func Calculate(durations []time.Duration) Result {

	sort.Slice(durations, func(i, j int) bool {
		return durations[i] < durations[j]
	})

	total := time.Duration(0)

	for _, d := range durations {
		total += d
	}

	avg := total / time.Duration(len(durations))

	p95 := durations[int(float64(len(durations))*0.95)]

	return Result{
		Avg: avg,
		P95: p95,
		Min: durations[0],
		Max: durations[len(durations)-1],
	}
}