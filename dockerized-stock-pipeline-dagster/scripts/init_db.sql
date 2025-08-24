CREATE SCHEMA IF NOT EXISTS market;

CREATE TABLE IF NOT EXISTS market.daily_prices (
    symbol          TEXT        NOT NULL,
    ts              TIMESTAMPTZ NOT NULL,
    open            NUMERIC,
    high            NUMERIC,
    low             NUMERIC,
    close           NUMERIC,
    volume          BIGINT,
    raw_payload     JSONB,
    inserted_at     TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, ts)
);
