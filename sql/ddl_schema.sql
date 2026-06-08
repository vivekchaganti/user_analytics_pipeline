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
    username VARCHAR(150) UNIQUE NOT NULL,
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