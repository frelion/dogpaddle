\set ON_ERROR_STOP on

CREATE SCHEMA sales;
CREATE SCHEMA ops;
CREATE TABLE sales.orders (
    order_id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    status TEXT NOT NULL,
    quantity BIGINT NOT NULL,
    unit_price_cents BIGINT NOT NULL
);
ALTER TABLE sales.orders REPLICA IDENTITY FULL;
CREATE PUBLICATION dogpaddle_demo_publication FOR TABLE sales.orders;
INSERT INTO sales.orders VALUES
    (5001, 100, 'paid', 10, 5000),
    (5002, 101, 'pending', 5, 3000);
