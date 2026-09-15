-- Enrich paid PostgreSQL orders with customer segments maintained in MySQL.
INSERT INTO postgres(
    connection => env('DOGPADDLE_CUSTOMER_ORDERS_TARGET'),
    table => 'analytics.enriched_orders'
)
WITH orders AS (
    SELECT order_id, customer_id, status
    FROM postgres_cdc(
        connection => env('DOGPADDLE_ORDERS_SOURCE'),
        table => 'sales.orders',
        publication => 'dogpaddle_customer_orders'
    )
),
customers AS (
    SELECT customer_id, segment
    FROM mysql_cdc(
        connection => env('DOGPADDLE_CUSTOMERS_SOURCE'),
        table => 'crm.customers'
    )
)
SELECT orders.order_id, orders.customer_id, customers.segment
FROM orders
JOIN customers
    ON orders.customer_id = customers.customer_id
WHERE orders.status = 'paid';
