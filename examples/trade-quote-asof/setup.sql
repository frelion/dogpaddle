\set ON_ERROR_STOP on

CREATE SCHEMA market;
CREATE SCHEMA analytics;

CREATE TABLE market.fills (
    trade_id BIGINT PRIMARY KEY,
    symbol TEXT NOT NULL,
    executed_at TIMESTAMP NOT NULL,
    side TEXT NOT NULL,
    price_cents BIGINT NOT NULL
);
ALTER TABLE market.fills REPLICA IDENTITY FULL;

CREATE TABLE market.quotes (
    quote_id BIGINT PRIMARY KEY,
    symbol TEXT NOT NULL,
    quoted_at TIMESTAMP NOT NULL,
    bid_cents BIGINT NOT NULL,
    ask_cents BIGINT NOT NULL
);
ALTER TABLE market.quotes REPLICA IDENTITY FULL;

CREATE PUBLICATION dogpaddle_fills FOR TABLE market.fills;
CREATE PUBLICATION dogpaddle_quotes FOR TABLE market.quotes;

INSERT INTO market.quotes VALUES
    (1, 'ACME', '2026-09-15 09:30:00.100', 10000, 10020);
INSERT INTO market.fills VALUES
    (9001, 'ACME', '2026-09-15 09:30:00.500', 'BUY', 10018);
