-- Создание таблицы с raw JSONB из Aviasales
CREATE TABLE IF NOT EXISTS flight_prices_calendar_raw (
    origin       TEXT NOT NULL,
    destination  TEXT NOT NULL,
    departure_at DATE NOT NULL,
    payload      JSONB,
    created_at   TIMESTAMPTZ,
    loaded_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (origin, destination, departure_at)
);

-- Таблица для распарсенных билетов
CREATE TABLE IF NOT EXISTS flight_offers (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    origin               TEXT,
    destination          TEXT,
    departure_date       DATE,
    origin_airport       TEXT,
    destination_airport  TEXT,
    price                NUMERIC,
    airline              TEXT,
    flight_number        TEXT,
    departure_at         TIMESTAMPTZ,
    return_at            TIMESTAMPTZ,
    currency             TEXT
);