-- Turn paid orders into a live fulfillment queue in the same PostgreSQL database.
INSERT INTO postgres(
    sink_id => 'fulfillment_queue', host => '127.0.0.1',
    port => env('DOGPADDLE_FULFILLMENT_PORT'), database => 'postgres',
    user => env('DOGPADDLE_FULFILLMENT_USER'), password => env('DOGPADDLE_FULFILLMENT_PASSWORD'),
    schema => 'ops', table => 'fulfillment_queue'
)
WITH raw_orders AS (
    SELECT order_id, customer, region, status,
        CAST(quantity AS BIGINT) AS quantity, unit_price_cents,
        CAST(discount_pct AS BIGINT) AS discount_pct
    FROM postgres_cdc(
        engine_name => 'orders', runtime_bundle => env('DOGPADDLE_FULFILLMENT_BUNDLE'),
        host => '127.0.0.1', port => env('DOGPADDLE_FULFILLMENT_PORT'), database => 'postgres',
        user => env('DOGPADDLE_FULFILLMENT_USER'), password => env('DOGPADDLE_FULFILLMENT_PASSWORD'),
        schema => 'sales', table => 'orders', slot => 'orders_slot', publication => 'orders_publication'
    )
),
priced AS (
    SELECT order_id, customer, region, status, discount_pct,
        quantity * unit_price_cents AS subtotal_cents
    FROM raw_orders
),
enriched AS (
    SELECT order_id, customer, region, status, subtotal_cents,
        subtotal_cents - (subtotal_cents * discount_pct / 100) AS payable_cents
    FROM priced
),
qualified AS (
    SELECT order_id, customer, region, subtotal_cents, payable_cents,
        CASE WHEN payable_cents >= 40000 THEN 'priority'
            ELSE 'standard' END AS handling_lane
    FROM enriched
    WHERE status = 'paid' AND payable_cents >= 10000
)
SELECT order_id, customer, subtotal_cents, payable_cents,
    'CN-HUB' AS fulfillment_center, handling_lane, 'regional_route' AS handling_reason
FROM qualified
WHERE region = 'cn-east' OR region = 'cn-south'
UNION ALL
SELECT order_id, customer, subtotal_cents, payable_cents,
    'GLOBAL-HUB' AS fulfillment_center, handling_lane,
    CASE WHEN payable_cents >= 40000 THEN 'export_review'
        ELSE NULL END AS handling_reason
FROM qualified
WHERE region <> 'cn-east' AND region <> 'cn-south';
