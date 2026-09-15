-- Keep a PostgreSQL serving copy in step with the source event table.
INSERT INTO postgres(
    connection => env('DOGPADDLE_EVENT_SYNC_TARGET'),
    table => 'warehouse.events_sync'
)
SELECT event_id, payload
FROM postgres_cdc(
    connection => env('DOGPADDLE_EVENT_SYNC_SOURCE'),
    table => 'sales.events',
    publication => 'dogpaddle_demo_publication'
);
