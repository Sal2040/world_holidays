CREATE DATABASE world_holidays;

\c world_holidays

CREATE TABLE IF NOT EXISTS holiday(
    holiday_id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,
    description VARCHAR,
    country VARCHAR NOT NULL,
    date TIMESTAMP WITH TIME ZONE NOT NULL,
    UNIQUE (name, country, date)
);

CREATE TABLE IF NOT EXISTS holiday_state_type(
    holiday_id INT REFERENCES holiday(holiday_id) NOT NULL,
    state VARCHAR NOT NULL,
    type VARCHAR NOT NULL,
    UNIQUE(holiday_id, state, type)
);