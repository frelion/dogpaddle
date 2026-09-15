\set ON_ERROR_STOP on

CREATE SCHEMA sales;
CREATE SCHEMA warehouse;
CREATE TABLE sales.events (
    event_id BIGINT PRIMARY KEY,
    payload TEXT NOT NULL
);
ALTER TABLE sales.events REPLICA IDENTITY FULL;
CREATE PUBLICATION dogpaddle_demo_publication FOR TABLE sales.events;
INSERT INTO sales.events VALUES (1, 'created'), (2, 'paid');
