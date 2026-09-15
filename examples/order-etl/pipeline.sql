-- Maintain the paid-order fulfillment relation shown in the README.
INSERT INTO postgres(
    connection => env('DOGPADDLE_ORDER_ETL_TARGET'),
    table => 'ops.fulfillment'
)
SELECT
    order_id,
    quantity * unit_price_cents AS amount,
    CASE WHEN quantity * unit_price_cents >= 40000
         THEN 'priority' ELSE 'standard' END AS lane
FROM postgres_cdc(
    connection => env('DOGPADDLE_ORDER_ETL_SOURCE'),
    table => 'sales.orders',
    publication => 'dogpaddle_demo_publication'
)
WHERE status = 'paid';
