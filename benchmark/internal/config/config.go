package config

import (
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type Config struct {
	DBType string

	Requests int
	Threads  int

	RunRead        bool
	RunConcurrency bool
	RunAnalytics   bool

	UseIndex     bool
	UsePartitionMonthly bool
	UsePartitionYearly bool

	PostgresDSN string

	MongoURI string
	MongoDB  string
}

func Load() *Config {

	godotenv.Load()

	req, _ := strconv.Atoi(os.Getenv("REQUESTS"))
	threads, _ := strconv.Atoi(os.Getenv("THREADS"))

	return &Config{
		DBType: os.Getenv("DB_TYPE"),

		Requests: req,
		Threads:  threads,

		RunRead:        os.Getenv("RUN_READ") == "true",
		RunConcurrency: os.Getenv("RUN_CONCURRENCY") == "true",
		RunAnalytics:   os.Getenv("RUN_ANALYTICS") == "true",

		UseIndex:     os.Getenv("USE_INDEX") == "true",
		UsePartitionMonthly: os.Getenv("USE_PARTITION_MONTHLY") == "true",
		UsePartitionYearly: os.Getenv("USE_PARTITION_YEARLY") == "true",

		PostgresDSN: os.Getenv("POSTGRES_DSN"),

		MongoURI: os.Getenv("MONGO_URI"),
		MongoDB:  os.Getenv("MONGO_DB"),
	}
}