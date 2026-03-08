package benchmark

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"benchmark/internal/db"
)

func RunConcurrency(db db.Database, threads int) {

	db.ResetStock(1, 1)
	stock, _ := db.GetStock(1)
	fmt.Println("Begin Stock:", stock)

	var wg sync.WaitGroup

	var success atomic.Int64
	var fail atomic.Int64
	var done atomic.Int64

	stop := make(chan struct{})

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				fmt.Printf("\rCONCURRENCY Progress: %d/%d", done.Load(), threads)
			case <-stop:
				return
			}
		}
	}()

	wg.Add(threads)

	for i := 0; i < threads; i++ {

		go func() {

			defer wg.Done()

			ok, err := db.BuyProduct(1)

			if err != nil {
				fmt.Println("\nBuyProduct error:", err)
			}

			if ok {
				success.Add(1)
			} else {
				fail.Add(1)
			}

			done.Add(1)

		}()
	}

	wg.Wait()

	close(stop)

	fmt.Printf("\rCONCURRENCY Progress: %d/%d\n", threads, threads)

	stock, _ = db.GetStock(1)

	fmt.Println("CONCURRENCY TEST")
	fmt.Println("Success:", success.Load())
	fmt.Println("Fail:", fail.Load())
	fmt.Println("Final Stock:", stock)
}