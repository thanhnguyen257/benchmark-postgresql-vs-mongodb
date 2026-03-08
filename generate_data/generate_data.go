package main

import (
    "bufio"
    "encoding/csv"
    "encoding/json"
    "fmt"
    "io"
    "log"
    "math"
    "math/rand"
    "os"
    "path/filepath"
    "runtime"
    "sort"
    "strconv"
    "sync"
    "time"
)

const (
    totalUsers    = 1_000_000
    totalProducts = 100_000
    totalOrders   = 10_000_000

    minItemsPerOrder = 1
    maxItemsPerOrder = 6

    writeBufferSize = 4 * 1024 * 1024 // 4MB
)

var weightedStatus = []string{
    "completed", "completed", "completed", "completed",
    "shipped", "shipped",
    "paid",
    "pending",
    "cancelled",
}

var userCreatedAt []time.Time

type UserDoc struct {
    UserID    int    `json:"userId"`
    Email     string `json:"email"`
    FirstName string `json:"first_name"`
    LastName  string `json:"last_name"`
    CreatedAt string `json:"createdAt"`
}

type ProductDoc struct {
    ProductID int     `json:"productId"`
    Name      string  `json:"name"`
    Price     float64 `json:"price"`
    Stock     int     `json:"stock"`
}

type OrderMongoDoc struct {
    OrderID     int          `json:"orderId"`
    OrderDate   string       `json:"orderDate"`
    TotalAmount float64      `json:"totalAmount"`
    Status      string       `json:"status"`
    User        OrderUser    `json:"user"`
    Items       []OrderItem  `json:"items"`
}

type OrderUser struct {
    UserID    int    `json:"userId"`
    Email     string `json:"email"`
    FirstName string `json:"firstName"`
    LastName  string `json:"lastName"`
}

type OrderItem struct {
    ProductID int     `json:"productId"`
    Name      string  `json:"name"`
    Quantity  int     `json:"quantity"`
    UnitPrice float64 `json:"unitPrice"`
}

func main() {
    // rand.Seed(time.Now().UnixNano())
    rand.Seed(420) // set for easy reproduce

    outputDir := "./generate_data/dummy_data"
    os.MkdirAll(outputDir, os.ModePerm)

    generateUsers(outputDir)
    generateProducts(outputDir)
    generateOrdersParallel(outputDir)

    err := mergePartFiles(
        outputDir,
        "orders",
        "csv",
        "order_id,user_id,order_date,total_amount,status\n",
    )
    if err != nil {
        log.Fatal(err)
    }

    err = mergePartFiles(
        outputDir,
        "orderitems",
        "csv",
        "order_item_id,order_id,product_id,quantity,unit_price\n",
    )
    if err != nil {
        log.Fatal(err)
    }

    err = mergePartFiles(
        outputDir,
        "orders_mongo",
        "json",
        "",
    )
    if err != nil {
        log.Fatal(err)
    }

    fmt.Println("DONE.")
}

func generateUsers(outputDir string) {

    csvFile, _ := os.Create(filepath.Join(outputDir, "users.csv"))
    defer csvFile.Close()
    csvWriter := csv.NewWriter(csvFile)
    defer csvWriter.Flush()

    jsonFile, _ := os.Create(filepath.Join(outputDir, "users.json"))
    defer jsonFile.Close()
    jsonWriter := bufio.NewWriterSize(jsonFile, writeBufferSize)
    jsonEncoder := json.NewEncoder(jsonWriter)
    defer jsonWriter.Flush()

    csvWriter.Write([]string{"user_id", "email", "first_name", "last_name", "created_at"})

    now := time.Now()
    twoYearsAgo := now.AddDate(-2, 0, 0)

    userCreatedAt = make([]time.Time, totalUsers)

    for i := 1; i <= totalUsers; i++ {
        created := randomTimeObj(twoYearsAgo, now)
        userCreatedAt[i-1] = created

        email := fmt.Sprintf("user%d@example.com", i)
        first := fmt.Sprintf("First%d", i)
        last := fmt.Sprintf("Last%d", i)

        csvWriter.Write([]string{
            strconv.Itoa(i),
            email,
            first,
            last,
            created.Format("2006-01-02 15:04:05"),
        })

        doc := UserDoc{
            UserID:    i,
            Email:     email,
            FirstName: first,
            LastName:  last,
            CreatedAt: created.Format(time.RFC3339),
        }

        jsonEncoder.Encode(doc)
    }
}

func generateProducts(outputDir string) {

    csvFile, _ := os.Create(filepath.Join(outputDir, "products.csv"))
    defer csvFile.Close()
    csvWriter := csv.NewWriter(csvFile)
    defer csvWriter.Flush()

    jsonFile, _ := os.Create(filepath.Join(outputDir, "products.json"))
    defer jsonFile.Close()
    jsonWriter := bufio.NewWriterSize(jsonFile, writeBufferSize)
    jsonEncoder := json.NewEncoder(jsonWriter)
    defer jsonWriter.Flush()

    csvWriter.Write([]string{"product_id", "name", "price", "stock"})

    for i := 1; i <= totalProducts; i++ {

        price := randomFloat(5, 2000)
        stock := rand.Intn(5000)
        name := fmt.Sprintf("Product %d", i)

        csvWriter.Write([]string{
            strconv.Itoa(i),
            name,
            fmt.Sprintf("%.2f", price),
            strconv.Itoa(stock),
        })

        doc := ProductDoc{
            ProductID: i,
            Name:      name,
            Price:     price,
            Stock:     stock,
        }

        jsonEncoder.Encode(doc)
    }
}

func generateOrdersParallel(outputDir string) {

    numWorkers := runtime.NumCPU()
    fmt.Println("Workers:", numWorkers)

    ordersPerWorker := totalOrders / numWorkers

    var wg sync.WaitGroup
    now := time.Now()

    for w := 0; w < numWorkers; w++ {

        wg.Add(1)

        go func(workerID int) {
            defer wg.Done()

            r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

            startID := workerID*ordersPerWorker + 1
            endID := startID + ordersPerWorker - 1

            if workerID == numWorkers-1 {
                endID = totalOrders
            }

            orderFile, _ := os.Create(filepath.Join(outputDir, fmt.Sprintf("orders_part_%d.csv", workerID)))
            itemFile, _ := os.Create(filepath.Join(outputDir, fmt.Sprintf("orderitems_part_%d.csv", workerID)))
            jsonFile, _ := os.Create(filepath.Join(outputDir, fmt.Sprintf("orders_mongo_part_%d.json", workerID)))

            defer orderFile.Close()
            defer itemFile.Close()
            defer jsonFile.Close()

            orderWriter := csv.NewWriter(bufio.NewWriterSize(orderFile, writeBufferSize))
            itemWriter := csv.NewWriter(bufio.NewWriterSize(itemFile, writeBufferSize))
            jsonWriter := bufio.NewWriterSize(jsonFile, writeBufferSize)
            jsonEncoder := json.NewEncoder(jsonWriter)

            var localOrderItemID int64 = int64(startID * 10)

            for orderID := startID; orderID <= endID; orderID++ {

                userIndex := r.Intn(totalUsers)
                userID := userIndex + 1
                userCreated := userCreatedAt[userIndex]

                orderDate := randomTimeObj(userCreated, now)
                status := weightedStatus[r.Intn(len(weightedStatus))]

                numItems := r.Intn(maxItemsPerOrder-minItemsPerOrder+1) + minItemsPerOrder

                totalAmount := 0.0
                items := make([]OrderItem, 0, numItems)

                for i := 0; i < numItems; i++ {

                    productID := r.Intn(totalProducts) + 1
                    qty := r.Intn(5) + 1
                    price := randomFloat(5, 2000)

                    totalAmount += float64(qty) * price

                    itemWriter.Write([]string{
                        strconv.FormatInt(localOrderItemID, 10),
                        strconv.Itoa(orderID),
                        strconv.Itoa(productID),
                        strconv.Itoa(qty),
                        fmt.Sprintf("%.2f", price),
                    })

                    items = append(items, OrderItem{
                        ProductID: productID,
                        Name:      fmt.Sprintf("Product %d", productID),
                        Quantity:  qty,
                        UnitPrice: price,
                    })

                    localOrderItemID++
                }

                orderWriter.Write([]string{
                    strconv.Itoa(orderID),
                    strconv.Itoa(userID),
                    orderDate.Format("2006-01-02 15:04:05"),
                    fmt.Sprintf("%.2f", totalAmount),
                    status,
                })

                doc := OrderMongoDoc{
                    OrderID:     orderID,
                    OrderDate:   orderDate.Format(time.RFC3339),
                    TotalAmount: totalAmount,
                    Status:      status,
                    User: OrderUser{
                        UserID:    userID,
                        Email:     fmt.Sprintf("user%d@example.com", userID),
                        FirstName: fmt.Sprintf("First%d", userID),
                        LastName:  fmt.Sprintf("Last%d", userID),
                    },
                    Items: items,
                }

                jsonEncoder.Encode(doc)
            }

            orderWriter.Flush()
            itemWriter.Flush()
            jsonWriter.Flush()

        }(w)
    }

    wg.Wait()
}

func randomTimeObj(start, end time.Time) time.Time {
    delta := end.Unix() - start.Unix()
    sec := rand.Int63n(delta)
    return time.Unix(start.Unix()+sec, 0)
}

func randomFloat(min, max float64) float64 {
    val := min + rand.Float64()*(max-min)
    return math.Round(val*100) / 100
}

func mergePartFiles(outputDir, prefix, extension, header string) error {

    finalPath := filepath.Join(outputDir, prefix+"."+extension)
    finalFile, err := os.Create(finalPath)
    if err != nil {
        return err
    }
    defer finalFile.Close()

    writer := bufio.NewWriterSize(finalFile, writeBufferSize)
    defer writer.Flush()

    if header != "" {
        if _, err := writer.WriteString(header); err != nil {
            return err
        }
    }

    pattern := filepath.Join(outputDir, prefix+"_part_*."+extension)
    files, err := filepath.Glob(pattern)
    if err != nil {
        return err
    }

    sort.Strings(files)

    for _, file := range files {

        f, err := os.Open(file)
        if err != nil {
            return err
        }

        if _, err := io.Copy(writer, f); err != nil {
            f.Close()
            return err
        }

        f.Close()

        if err := os.Remove(file); err != nil {
            return err
        }
    }

    fmt.Println("Merged:", prefix+"."+extension)
    return nil
}