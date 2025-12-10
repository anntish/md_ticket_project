CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.aviasales_api_log (
    id BIGSERIAL PRIMARY KEY,

    -- Mongo _id
    aviasales_id TEXT NOT NULL,

    -- поля из Mongo-документа
    endpoint TEXT,
    request JSONB,
    response JSONB,
    status_code INTEGER,
    error BOOLEAN,

    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    batch_started_at TIMESTAMPTZ,

    -- SCD2-поля
    valid_from_dttm TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    valid_to_dttm   TIMESTAMPTZ NOT NULL DEFAULT '5999-12-31'
);

-- Индексы под аналитику и upsert
CREATE INDEX IF NOT EXISTS idx_aviasales_api_log_aviasales_id
    ON raw.aviasales_api_log (aviasales_id);

CREATE INDEX IF NOT EXISTS idx_aviasales_api_log_created_at
    ON raw.aviasales_api_log (created_at);

CREATE INDEX IF NOT EXISTS idx_aviasales_api_log_status_code
    ON raw.aviasales_api_log (status_code);

CREATE INDEX IF NOT EXISTS idx_aviasales_api_log_batch_started_at
    ON raw.aviasales_api_log (batch_started_at);
