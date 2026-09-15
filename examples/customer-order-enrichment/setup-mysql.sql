CREATE DATABASE crm;
USE crm;

CREATE TABLE customers (
    customer_id BIGINT PRIMARY KEY,
    segment VARCHAR(32) NOT NULL
) ENGINE = InnoDB;

INSERT INTO customers VALUES
    (100, 'standard'),
    (101, 'standard');
