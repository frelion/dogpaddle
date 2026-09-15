-- Match every fill with the latest quote visible at execution time.
INSERT INTO postgres(
    connection => env('DOGPADDLE_EXECUTION_QUALITY_TARGET'),
    table => 'analytics.execution_quality'
)
SELECT
    fills.trade_id,
    fills.symbol,
    fills.executed_at,
    fills.side,
    fills.price_cents AS execution_price_cents,
    quotes.quoted_at,
    (quotes.bid_cents + quotes.ask_cents) / 2 AS arrival_mid_cents,
    CASE
        WHEN fills.side = 'BUY' THEN fills.price_cents - (quotes.bid_cents + quotes.ask_cents) / 2
        ELSE (quotes.bid_cents + quotes.ask_cents) / 2 - fills.price_cents
    END AS slippage_cents
FROM postgres_cdc(
    connection => env('DOGPADDLE_FILLS_SOURCE'),
    table => 'market.fills',
    publication => 'dogpaddle_fills'
) AS fills
ASOF JOIN postgres_cdc(
    connection => env('DOGPADDLE_QUOTES_SOURCE'),
    table => 'market.quotes',
    publication => 'dogpaddle_quotes'
) AS quotes
MATCH_CONDITION (fills.executed_at >= quotes.quoted_at)
ON fills.symbol = quotes.symbol;
