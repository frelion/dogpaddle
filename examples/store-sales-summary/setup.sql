\set ON_ERROR_STOP on

CREATE SCHEMA sales;
CREATE SCHEMA analytics;

CREATE TABLE sales.store_orders (
    order_id BIGINT PRIMARY KEY,
    store_id BIGINT NOT NULL,
    status TEXT NOT NULL,
    amount_cents BIGINT NOT NULL
);
ALTER TABLE sales.store_orders REPLICA IDENTITY FULL;
CREATE PUBLICATION dogpaddle_store_sales FOR TABLE sales.store_orders;

INSERT INTO sales.store_orders VALUES
    (3001, 11, 'paid', 12000),
    (3002, 11, 'paid', 18000),
    (3003, 12, 'pending', 25000);
