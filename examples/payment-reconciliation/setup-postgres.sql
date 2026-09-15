\set ON_ERROR_STOP on

CREATE SCHEMA payments;
CREATE SCHEMA reconciliation;

CREATE TABLE payments.payment_orders (
    payment_id BIGINT PRIMARY KEY,
    merchant_id BIGINT NOT NULL,
    expected_cents BIGINT NOT NULL
);
ALTER TABLE payments.payment_orders REPLICA IDENTITY FULL;
CREATE PUBLICATION dogpaddle_payment_orders FOR TABLE payments.payment_orders;

INSERT INTO payments.payment_orders VALUES
    (7001, 41, 12500),
    (7002, 41, 8900);
