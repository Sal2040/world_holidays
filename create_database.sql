CREATE DATABASE world_holidays;

\c world_holidays

CREATE TABLE IF NOT EXISTS holiday(
    holiday_id SERIAL PRIMARY KEY,
    name VARCHAR,
    description VARCHAR,
    country VARCHAR,
    date TIMESTAMP WITH TIME ZONE,
    UNIQUE (name, country, date)
);

CREATE TABLE IF NOT EXISTS holiday_state_type(
    holiday_id INT REFERENCES holiday(holiday_id),
    state VARCHAR,
    type VARCHAR
);