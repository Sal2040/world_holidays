CREATE DATABASE world_holidays;

\c world_holidays

CREATE TABLE IF NOT EXISTS holiday(
    holiday_id INT,
    name VARCHAR,
    description VARCHAR,
    country VARCHAR,
    date TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (holiday_id, name, date)
);

CREATE TABLE IF NOT EXISTS holiday_state_type(
    holiday_id INT REFERENCES holiday(holiday_id),
    state VARCHAR,
    type VARCHAR,
);

CREATE TABLE IF NOT EXISTS holiday_state(
    holiday_id INT REFERENCES holiday(holiday_id),
    state VARCHAR,
    PRIMARY KEY (holiday_id, state)
);