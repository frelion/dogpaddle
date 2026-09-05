CREATE TABLE public.d1_events (
    id BIGINT PRIMARY KEY,
    tx_seq INTEGER NOT NULL,
    payload TEXT NOT NULL
);

ALTER TABLE public.d1_events REPLICA IDENTITY FULL;

CREATE PUBLICATION dogpaddle_d1_publication FOR TABLE public.d1_events;
