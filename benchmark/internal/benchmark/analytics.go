package benchmark

import (
	"fmt"
	"time"

	"benchmark/internal/db"
)

func RunAnalytics(db db.Database, table string) {

	start := time.Now()

	db.RevenueByMonth(table)

	fmt.Println("Analytics query time:", time.Since(start))

	explain, _ := db.ExplainRevenue(table)

	fmt.Println("Explain plan:")
	fmt.Println(explain)
}