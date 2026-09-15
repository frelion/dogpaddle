-- Reconcile business payments in PostgreSQL with settlements arriving in MySQL.
INSERT INTO postgres(
    connection => env('DOGPADDLE_RECONCILIATION_TARGET'),
    table => 'reconciliation.payment_status'
)
WITH payments AS (
    SELECT payment_id, merchant_id, expected_cents
    FROM postgres_cdc(
        connection => env('DOGPADDLE_PAYMENTS_SOURCE'),
        table => 'payments.payment_orders',
        publication => 'dogpaddle_payment_orders'
    )
),
settlements AS (
    SELECT payment_id, settled_cents
    FROM mysql_cdc(
        connection => env('DOGPADDLE_SETTLEMENTS_SOURCE'),
        table => 'dogpaddle.settlements'
    )
)
SELECT
    payments.payment_id AS business_payment_id,
    settlements.payment_id AS settlement_payment_id,
    payments.merchant_id,
    payments.expected_cents,
    settlements.settled_cents,
    CASE
        WHEN payments.payment_id IS NULL THEN 'missing_business_payment'
        WHEN settlements.payment_id IS NULL THEN 'awaiting_settlement'
        WHEN payments.expected_cents = settlements.settled_cents THEN 'matched'
        ELSE 'amount_mismatch'
    END AS reconciliation_status
FROM payments
FULL OUTER JOIN settlements
    ON payments.payment_id = settlements.payment_id;
