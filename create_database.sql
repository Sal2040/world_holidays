\c world_holidays

CREATE TABLE IF NOT EXISTS holiday(
    holiday_id INT PRIMARY KEY,
    name VARCHAR,
    description VARCHAR,
    country VARCHAR,
    date TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS holiday_type(
    holiday_id INT REFERENCES holiday(holiday_id),
    type VARCHAR,
    PRIMARY KEY (holiday_id, type)
);

CREATE TABLE IF NOT EXISTS holiday_location(
    holiday_id INT REFERENCES holiday(holiday_id),
    location VARCHAR,
    PRIMARY KEY (holiday_id, location)
);

CREATE TABLE IF NOT EXISTS holiday_state(
    holiday_id INT REFERENCES holiday(holiday_id),
    state VARCHAR,
    PRIMARY KEY (holiday_id, state)
);