CREATE DATABASE dogpaddle;
USE dogpaddle;

CREATE TABLE settlements (
    payment_id BIGINT PRIMARY KEY,
    settled_cents BIGINT NOT NULL
) ENGINE = InnoDB;
