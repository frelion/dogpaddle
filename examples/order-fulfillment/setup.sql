\set ON_ERROR_STOP on

CREATE SCHEMA sales;
CREATE SCHEMA ops;

CREATE TABLE sales.orders (
    order_id BIGINT PRIMARY KEY,
    customer TEXT NOT NULL,
    region TEXT NOT NULL,
    status TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price_cents BIGINT NOT NULL,
    discount_pct INTEGER NOT NULL
);
ALTER TABLE sales.orders REPLICA IDENTITY FULL;
CREATE PUBLICATION orders_publication FOR TABLE sales.orders;

INSERT INTO sales.orders VALUES
    (101, 'Acme', 'cn-east', 'new', 3, 5000, 10),
    (102, 'Orbit', 'eu-west', 'paid', 2, 4000, 0),
    (103, 'Nova', 'cn-south', 'paid', 4, 8000, 25);
