-- Maintain one current sales summary for every store.
INSERT INTO postgres(
    connection => env('DOGPADDLE_STORE_SALES_TARGET'),
    table => 'analytics.store_sales'
)
SELECT
    store_id,
    COUNT(*) AS order_count,
    SUM(amount_cents) AS revenue_cents,
    AVG(amount_cents) AS average_order_cents,
    MIN(amount_cents) AS smallest_order_cents,
    MAX(amount_cents) AS largest_order_cents
FROM postgres_cdc(
    connection => env('DOGPADDLE_STORE_SALES_SOURCE'),
    table => 'sales.store_orders',
    publication => 'dogpaddle_store_sales'
)
WHERE status = 'paid'
GROUP BY store_id;
