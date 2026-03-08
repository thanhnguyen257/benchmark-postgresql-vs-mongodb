package db

type Database interface {
	GetOrder(id int64, table string) error

	BuyProduct(productID int64) (bool, error)

	ResetStock(productID int64, stock int) error

	GetStock(productID int64) (int, error)

	RevenueByMonth(table string) error

	ExplainRevenue(table string) (string, error)

	CreateIndexes() error
	DropIndexes() error

	Close()
}