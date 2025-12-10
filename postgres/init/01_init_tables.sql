CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.aviasales_raw_offers (
    id         BIGSERIAL PRIMARY KEY,
    raw_offer  JSONB NOT NULL,
    loaded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
