package postgres

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Postgres struct {
	pool *pgxpool.Pool
}

func New(dsn string) (*Postgres, error) {
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		return nil, err
	}

	if err := pool.Ping(context.Background()); err != nil {
		return nil, err
	}

	return &Postgres{pool}, nil
}

func (p *Postgres) Close() {
	p.pool.Close()
}

func (p *Postgres) GetOrder(id int64, table string) error {

	query := fmt.Sprintf(`
		SELECT o.order_id
		FROM %s o
		JOIN users u ON u.user_id=o.user_id
		JOIN orderitems oi ON oi.order_id=o.order_id
		WHERE o.order_id=$1`, table)

	var orderID int64
	err := p.pool.QueryRow(context.Background(), query, id).Scan(&orderID)

	return err
}

func (p *Postgres) BuyProduct(id int64) (bool, error) {

	cmd, err := p.pool.Exec(context.Background(),
		`UPDATE products
		 SET stock=stock-1
		 WHERE product_id=$1 AND stock>0`, id)

	if err != nil {
		return false, err
	}

	return cmd.RowsAffected() > 0, nil
}

func (p *Postgres) ResetStock(id int64, q int) error {

	_, err := p.pool.Exec(context.Background(),
		`UPDATE products SET stock=$2 WHERE product_id=$1`,
		id, q)

	return err
}

func (p *Postgres) GetStock(id int64) (int, error) {

	var q int

	err := p.pool.QueryRow(context.Background(),
		`SELECT stock FROM products WHERE product_id=$1`, id).Scan(&q)

	return q, err
}

func (p *Postgres) RevenueByMonth(table string) error {

	query := fmt.Sprintf(`
		SELECT date_trunc('month',order_date),SUM(total_amount)
		FROM %s
		GROUP BY 1`, table)
	_, err := p.pool.Exec(context.Background(), query)

	return err
}

func (p *Postgres) ExplainRevenue(table string) (string, error) {

	var plan string
	query := fmt.Sprintf(`
		EXPLAIN ANALYZE
		SELECT date_trunc('month',order_date),SUM(total_amount)
		FROM %s
		GROUP BY 1`, table)
	err := p.pool.QueryRow(context.Background(), query).Scan(&plan)

	return plan, err
}

func (p *Postgres) CreateIndexes() error {
	query := `
	CREATE INDEX IF NOT EXISTS idx_orders_user_id 
	ON orders(user_id);

	CREATE INDEX IF NOT EXISTS idx_orders_order_date 
	ON orders(order_date);

	CREATE INDEX IF NOT EXISTS idx_orderitems_order_id 
	ON orderitems(order_id);

	CREATE INDEX IF NOT EXISTS idx_orderitems_product_id 
	ON orderitems(product_id);
	`

	_, err := p.pool.Exec(context.Background(), query)
	return err
}

func (p *Postgres) DropIndexes() error {
	query := `
	DROP INDEX IF EXISTS idx_orders_user_id;
	DROP INDEX IF EXISTS idx_orders_order_date;
	DROP INDEX IF EXISTS idx_orderitems_order_id;
	DROP INDEX IF EXISTS idx_orderitems_product_id;
	`

	_, err := p.pool.Exec(context.Background(), query)
	return err
}