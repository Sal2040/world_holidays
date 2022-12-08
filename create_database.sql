\c world_holidays

CREATE TABLE IF NOT EXISTS holiday(
    holiday_id INT PRIMARY KEY,
    name VARCHAR,
    description VARCHAR,
    country_id CHAR(2),
    date TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS country(
    country_id CHAR(2) PRIMARY KEY,
    name VARCHAR
);

CREATE TABLE IF NOT EXISTS state(
    state_id CHAR(5) PRIMARY KEY,
    abbrev  VARCHAR,
    name VARCHAR,
    county_id CHAR(2)
);

CREATE TABLE IF NOT EXISTS location(
    location_id INT PRIMARY KEY,
    name VARCHAR,
    country_id CHAR(2)
);

CREATE TABLE IF NOT EXISTS type(
    type_id INT PRIMARY KEY,
    name VARCHAR
);

CREATE TABLE IF NOT EXISTS holiday_type(
    holiday_id INT REFERENCES holiday(holiday_id),
    type_id INT REFERENCES type(type_id),
    PRIMARY KEY (holiday_id, type_id)
);

CREATE TABLE IF NOT EXISTS holiday_location(
    holiday_id INT REFERENCES holiday(holiday_id),
    location_id INT REFERENCES location(location_id),
    PRIMARY KEY (holiday_id, location_id)
);

CREATE TABLE IF NOT EXISTS holiday_state(
    holiday_id INT REFERENCES holiday(holiday_id),
    state_id CHAR(5) REFERENCES state (state_id),
    PRIMARY KEY (holiday_id, state_id)
);