package main

import (
	"fmt"

	"benchmark/internal/benchmark"
	"benchmark/internal/config"
	"benchmark/internal/db"
	"benchmark/internal/db/mongo"
	"benchmark/internal/db/postgres"
)

func main() {

	cfg := config.Load()

	var database db.Database
	var err error
	var table string

	if cfg.DBType == "postgres" {

		database, err = postgres.New(cfg.PostgresDSN)

	} else {

		database, err = mongo.New(cfg.MongoURI, cfg.MongoDB)
	}

	if err != nil {
		panic(err)
	}

	defer database.Close()

	if cfg.UseIndex {
		database.CreateIndexes()
	} else {
		database.DropIndexes()
	}

	if cfg.UsePartitionMonthly {
		table = "orders_monthly"
	} else if cfg.UsePartitionYearly {
		table = "orders_yearly"
	} else {
		table = "orders"
	}

	if cfg.RunRead {
		benchmark.RunRead(database, cfg.Requests, int64(cfg.Requests), table)
	}

	if cfg.RunConcurrency {
		benchmark.RunConcurrency(database, cfg.Threads)
	}

	if cfg.RunAnalytics {
		benchmark.RunAnalytics(database, table)
	}

	fmt.Println("Benchmark finished")
}