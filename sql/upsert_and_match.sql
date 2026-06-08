-- UPSERT for dim_users (SCD Type 1) - Cast uuid text to UUID type
INSERT INTO dim_users (user_uuid, username, title, first_name, last_name, gender, email, phone, cell, nationality)
SELECT 
    u.uuid::UUID, 
    u.username, 
    u.title, 
    u.first_name, 
    u.last_name, 
    u.gender, 
    u.email, 
    u.phone, 
    u.cell, 
    u.nationality
FROM staging_users u
ON CONFLICT (user_uuid) DO UPDATE
SET 
    title = EXCLUDED.title,
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    gender = EXCLUDED.gender,
    email = EXCLUDED.email,
    phone = EXCLUDED.phone,
    cell = EXCLUDED.cell,
    nationality = EXCLUDED.nationality,
    updated_at = CURRENT_TIMESTAMP;

-- Create dim_locations via natural key deduplication
INSERT INTO dim_locations (street_number, street_name, city, state, country, postcode, latitude, longitude, timezone_offset, timezone_desc)
SELECT 
    u.street_number, u.street_name, u.city, u.state, u.country,
    u.postcode, u.latitude, u.longitude, u.timezone_offset, u.timezone_desc
FROM staging_users u
ON CONFLICT (street_number, street_name, postcode) 
DO NOTHING;

-- Populate dim_dates (pre-fill 2006-2026)
INSERT INTO dim_dates (date_key, full_date, year, month, day, quarter, day_of_week, is_weekend)
SELECT 
    EXTRACT(EPOCH FROM full_date)::INT,
    full_date,
    EXTRACT(YEAR FROM full_date)::INT,
    EXTRACT(MONTH FROM full_date)::INT,
    EXTRACT(DAY FROM full_date)::INT,
    EXTRACT(QUARTER FROM full_date)::INT,
    TO_CHAR(full_date, 'Day'),
    EXTRACT(ISODOW FROM full_date) IN (6,7)
FROM (
    SELECT NOW()::DATE - INTERVAL '20 years' + GENERATE_SERIES(0, 7300) AS full_date
) AS all_dates
WHERE full_date BETWEEN '2006-01-01' AND '2026-12-31'
ON CONFLICT (full_date) DO NOTHING;

-- Populate fact table
INSERT INTO fact_user_registrations (
    user_uuid, location_key, dob_date_key, reg_date_key,
    age_at_registration, current_age, registered_years
)
SELECT
    u.uuid::UUID,
    l.location_key,
    d1.date_key,
    d2.date_key,
    EXTRACT(YEAR FROM AGE(d2.full_date, d1.full_date))::INT,
    EXTRACT(YEAR FROM AGE(NOW(), d1.full_date))::INT,
    EXTRACT(YEAR FROM AGE(d2.full_date, d1.full_date))::INT
FROM staging_users u
JOIN dim_locations l 
    ON l.street_number = u.street_number
    AND l.street_name = u.street_name
    AND l.postcode = u.postcode
JOIN dim_dates d1 
    ON d1.full_date = u.dob_date::DATE
JOIN dim_dates d2 
    ON d2.full_date = u.registered_date::DATE
ON CONFLICT (user_uuid) DO NOTHING;