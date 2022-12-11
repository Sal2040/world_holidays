CREATE DATABASE world_holidays;

\c world_holidays

CREATE TABLE IF NOT EXISTS holiday(
    holiday_id INT PRIMARY KEY,
    name VARCHAR,
    description VARCHAR,
    country VARCHAR,
    date TIMESTAMP WITH TIME ZONE,
    UNIQUE (holiday_id, name, date)
);

CREATE TABLE IF NOT EXISTS holiday_state_type(
    holiday_id INT REFERENCES holiday(holiday_id),
    state VARCHAR,
    type VARCHAR
);