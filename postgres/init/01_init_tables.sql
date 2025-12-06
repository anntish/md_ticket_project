CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE raw.aviasales_flight_offers (
    mongo_id          text PRIMARY KEY,
    response          jsonb NOT NULL,
    mongo_created_at  text
);
