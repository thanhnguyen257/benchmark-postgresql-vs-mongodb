#!/bin/bash

set -e

CONN_STR="postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/postgres"

echo "Initializing PostgreSQL database..."

psql --set ON_ERROR_STOP=1 "$CONN_STR" -tc \
"SELECT 1 FROM pg_database WHERE datname='${POSTGRES_DB}'" | grep -q 1 || \
psql "$CONN_STR" -c "CREATE DATABASE ${POSTGRES_DB}"

POSTGRES_CONN_STR="postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}"

psql "$POSTGRES_CONN_STR" <<-EOSQL

CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
    product_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL,
    price NUMERIC(12,2) NOT NULL CHECK (price >= 0),
    stock INTEGER NOT NULL CHECK (stock >= 0)
);

CREATE TABLE IF NOT EXISTS orders (
    order_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id BIGINT NOT NULL,
    order_date TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    total_amount NUMERIC(14,2) NOT NULL CHECK (total_amount >= 0),
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

CREATE TABLE IF NOT EXISTS orderitems (
    order_item_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
    FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id);
-- CREATE INDEX IF NOT EXISTS idx_orderitems_order_id ON orderitems(order_id);
-- CREATE INDEX IF NOT EXISTS idx_orderitems_product_id ON orderitems(product_id);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO ${POSTGRES_USER};

EOSQL

psql "$POSTGRES_CONN_STR" \
-c "\copy users FROM '/dummy_data/users.csv' CSV HEADER"
psql "$POSTGRES_CONN_STR" \
-c "\copy products FROM '/dummy_data/products.csv' CSV HEADER"
psql "$POSTGRES_CONN_STR" \
-c "\copy orders FROM '/dummy_data/orders.csv' CSV HEADER"
psql "$POSTGRES_CONN_STR" \
-c "\copy orderitems FROM '/dummy_data/orderitems.csv' CSV HEADER"

psql "$POSTGRES_CONN_STR" <<'EOSQL'

CREATE TABLE orders_monthly (
    order_id BIGINT,
    user_id BIGINT,
    order_date TIMESTAMP,
    total_amount NUMERIC(14,2),
    status VARCHAR(50)
) PARTITION BY RANGE (order_date);

CREATE TABLE orders_yearly (
    order_id BIGINT,
    user_id BIGINT,
    order_date TIMESTAMP,
    total_amount NUMERIC(14,2),
    status VARCHAR(50)
) PARTITION BY RANGE (order_date);

-- Monthly partitions
DO $$
DECLARE
    start_date DATE;
    end_date DATE;
    d DATE;
BEGIN
    SELECT date_trunc('month', MIN(order_date))::date,
           date_trunc('month', MAX(order_date))::date
    INTO start_date, end_date
    FROM orders;

    d := start_date;

    WHILE d <= end_date LOOP
        EXECUTE format(
            'CREATE TABLE orders_m_%s PARTITION OF orders_monthly
             FOR VALUES FROM (%L) TO (%L);',
            to_char(d,'YYYY_MM'),
            d,
            d + INTERVAL '1 month'
        );

        d := d + INTERVAL '1 month';
    END LOOP;
END $$;

-- Yearly partitions
DO $$
DECLARE
    start_year INT;
    end_year INT;
    y INT;
BEGIN
    SELECT EXTRACT(YEAR FROM MIN(order_date)),
           EXTRACT(YEAR FROM MAX(order_date))
    INTO start_year, end_year
    FROM orders;

    FOR y IN start_year..end_year LOOP
        EXECUTE format(
            'CREATE TABLE orders_y_%s PARTITION OF orders_yearly
             FOR VALUES FROM (%L) TO (%L);',
            y,
            y || '-01-01',
            (y+1) || '-01-01'
        );
    END LOOP;
END $$;

-- Copy data into partition tables
INSERT INTO orders_monthly
SELECT order_id,user_id,order_date,total_amount,status FROM orders;

INSERT INTO orders_yearly
SELECT order_id,user_id,order_date,total_amount,status FROM orders;

EOSQL

echo "PostgreSQL initialization completed!"