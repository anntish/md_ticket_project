CREATE SCHEMA IF NOT EXISTS aviasales;

CREATE TABLE IF NOT EXISTS aviasales.aviasales_flight_offers (
    origin                  TEXT NOT NULL,
    destination             TEXT NOT NULL,
    origin_airport          TEXT,
    destination_airport     TEXT,
    price                   INTEGER,
    currency                TEXT,
    airline                 TEXT,
    flight_number           TEXT,
    departure_at            TIMESTAMPTZ NOT NULL,
    return_at               TIMESTAMPTZ,
    transfers               INTEGER,
    return_transfers        INTEGER,
    duration                INTEGER,
    duration_to             INTEGER,
    duration_back           INTEGER,
    link                    TEXT,
    calendar_departure_date DATE,

    mongo_created_at        TIMESTAMPTZ NOT NULL,

    PRIMARY KEY (
        origin,
        destination,
        departure_at,
        mongo_created_at
    )
);
