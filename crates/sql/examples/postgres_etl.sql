-- PostgreSQL CDC -> multi-stage ETL -> PostgreSQL, in one statement.
INSERT INTO postgres(
    sink_id => 'order_insights',
    host => '127.0.0.1',
    port => env('DOGPADDLE_POSTGRES_ETL_PORT'),
    database => 'postgres',
    user => env('DOGPADDLE_POSTGRES_ETL_USER'),
    password => env('DOGPADDLE_POSTGRES_ETL_PASSWORD'),
    schema => 'analytics',
    table => 'order_insights'
)
WITH raw_orders AS (
    SELECT
        order_id,
        customer,
        region,
        status,
        CAST(quantity AS BIGINT) AS quantity,
        unit_price_cents,
        CAST(discount_pct AS BIGINT) AS discount_pct
    FROM postgres_cdc(
        engine_name => 'orders',
        runtime_bundle => env('DOGPADDLE_POSTGRES_ETL_BUNDLE'),
        host => '127.0.0.1',
        port => env('DOGPADDLE_POSTGRES_ETL_PORT'),
        database => 'postgres',
        user => env('DOGPADDLE_POSTGRES_ETL_USER'),
        password => env('DOGPADDLE_POSTGRES_ETL_PASSWORD'),
        schema => 'sales',
        table => 'orders',
        slot => 'orders_slot',
        publication => 'orders_publication'
    )
),
priced AS (
    SELECT
        order_id,
        customer,
        region,
        status,
        discount_pct,
        quantity * unit_price_cents AS gross_cents
    FROM raw_orders
),
enriched AS (
    SELECT
        order_id,
        customer,
        region,
        status,
        gross_cents,
        gross_cents - (gross_cents * discount_pct / 100) AS net_cents
    FROM priced
),
qualified AS (
    SELECT
        order_id,
        customer,
        region,
        gross_cents,
        net_cents,
        CASE
            WHEN net_cents >= 40000 THEN 'priority'
            ELSE 'standard'
        END AS lane
    FROM enriched
    WHERE status = 'paid' AND net_cents >= 10000
)
SELECT
    order_id,
    customer,
    'China' AS market,
    gross_cents,
    net_cents,
    lane,
    'regional' AS attention
FROM qualified
WHERE region = 'cn-east' OR region = 'cn-south'
UNION ALL
SELECT
    order_id,
    customer,
    'Global' AS market,
    gross_cents,
    net_cents,
    lane,
    CASE
        WHEN net_cents >= 40000 THEN 'review'
        ELSE NULL
    END AS attention
FROM qualified
WHERE region <> 'cn-east' AND region <> 'cn-south';
