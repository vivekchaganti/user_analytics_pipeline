"""Warehouse DDL and per-table transform SQL used by the Airflow DAG."""

ENSURE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS staging_users (
    uuid VARCHAR(255),
    username VARCHAR(255),
    title VARCHAR(50),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    gender VARCHAR(50),
    email VARCHAR(255),
    phone VARCHAR(100),
    cell VARCHAR(100),
    nationality VARCHAR(10),
    street_number INT,
    street_name VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(255),
    country VARCHAR(255),
    postcode VARCHAR(100),
    latitude VARCHAR(100),
    longitude VARCHAR(100),
    timezone_offset VARCHAR(50),
    timezone_desc VARCHAR(255),
    dob_date VARCHAR(100),
    dob_age INT,
    registered_date VARCHAR(100),
    registered_age INT,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_users (
    user_uuid VARCHAR(255) PRIMARY KEY,
    username VARCHAR(150) NOT NULL,
    title VARCHAR(50),
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    gender VARCHAR(50),
    email VARCHAR(255),
    phone VARCHAR(100),
    cell VARCHAR(100),
    nationality VARCHAR(10),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_locations (
    location_key SERIAL PRIMARY KEY,
    street_number INT,
    street_name VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(255),
    country VARCHAR(255),
    postcode VARCHAR(100),
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6),
    timezone_offset VARCHAR(50),
    timezone_desc VARCHAR(255),
    UNIQUE (street_number, street_name, postcode)
);

CREATE TABLE IF NOT EXISTS dim_dates (
    date_key INT PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    quarter INT NOT NULL,
    day_of_week VARCHAR(15) NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_user_registrations (
    registration_id SERIAL PRIMARY KEY,
    user_uuid VARCHAR(255) REFERENCES dim_users(user_uuid) ON DELETE CASCADE,
    location_key INT REFERENCES dim_locations(location_key),
    dob_date_key INT REFERENCES dim_dates(date_key),
    reg_date_key INT REFERENCES dim_dates(date_key),
    age_at_registration INT,
    current_age INT,
    registered_years INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (user_uuid)
);
"""

UPSERT_DIM_USERS_SQL = """
INSERT INTO dim_users
(user_uuid, username, title, first_name, last_name, gender, email, phone,
 cell, nationality)
SELECT u.uuid, u.username, u.title, u.first_name, u.last_name, u.gender,
       u.email, u.phone, u.cell, u.nationality
FROM staging_users u
ON CONFLICT (user_uuid) DO UPDATE SET
    title = EXCLUDED.title,
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    gender = EXCLUDED.gender,
    email = EXCLUDED.email,
    phone = EXCLUDED.phone,
    cell = EXCLUDED.cell,
    nationality = EXCLUDED.nationality,
    updated_at = CURRENT_TIMESTAMP;
"""

UPSERT_DIM_LOCATIONS_SQL = """
INSERT INTO dim_locations
(street_number, street_name, city, state, country, postcode, latitude,
 longitude, timezone_offset, timezone_desc)
SELECT u.street_number, u.street_name, u.city, u.state, u.country,
       u.postcode, u.latitude::NUMERIC, u.longitude::NUMERIC,
       u.timezone_offset, u.timezone_desc
FROM staging_users u
ON CONFLICT (street_number, street_name, postcode) DO NOTHING;
"""

UPSERT_DIM_DATES_SQL = """
INSERT INTO dim_dates
(date_key, full_date, year, month, day, quarter, day_of_week, is_weekend)
SELECT
    (EXTRACT(EPOCH FROM d)::INT) AS date_key,
    d AS full_date,
    EXTRACT(YEAR FROM d)::INT,
    EXTRACT(MONTH FROM d)::INT,
    EXTRACT(DAY FROM d)::INT,
    EXTRACT(QUARTER FROM d)::INT,
    TO_CHAR(d, 'Day') AS day_of_week,
    (EXTRACT(ISODOW FROM d) IN (6, 7))::BOOLEAN AS is_weekend
FROM generate_series(
    '2006-01-01'::date, '2026-12-31'::date, interval '1 day'
) AS d
ON CONFLICT (full_date) DO NOTHING;
"""

UPSERT_FACT_USER_REGISTRATIONS_SQL = """
INSERT INTO fact_user_registrations
(user_uuid, location_key, dob_date_key, reg_date_key, age_at_registration,
 current_age, registered_years)
SELECT u.uuid, l.location_key, d1.date_key, d2.date_key,
       (EXTRACT(YEAR FROM AGE(d2.full_date, d1.full_date)))::INT,
       (EXTRACT(YEAR FROM AGE(NOW(), d1.full_date)))::INT,
       (EXTRACT(YEAR FROM AGE(d2.full_date, d1.full_date)))::INT
FROM staging_users u
JOIN dim_locations l ON l.street_number = u.street_number
    AND l.street_name = u.street_name AND l.postcode = u.postcode
JOIN dim_dates d1 ON d1.full_date = u.dob_date::DATE
JOIN dim_dates d2 ON d2.full_date = u.registered_date::DATE
ON CONFLICT (user_uuid) DO NOTHING;
"""
