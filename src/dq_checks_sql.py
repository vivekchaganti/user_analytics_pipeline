"""Staging DQ SQL checks. Each query must return a single TRUE/FALSE value."""

DATE_PATTERN = r"^\d{4}-\d{2}-\d{2}$"

STAGING_DQ_CHECKS = {
    "dq_staging_not_empty": """
        SELECT COUNT(*) > 0
        FROM staging_users
    """,
    "dq_no_null_uuid": """
        SELECT COUNT(*) = 0
        FROM staging_users
        WHERE uuid IS NULL OR TRIM(uuid) = ''
    """,
    "dq_no_duplicate_uuid": """
        SELECT COUNT(*) = 0
        FROM (
            SELECT uuid
            FROM staging_users
            GROUP BY uuid
            HAVING COUNT(*) > 1
        ) duplicates
    """,
    "dq_required_fields_present": """
        SELECT COUNT(*) = 0
        FROM staging_users
        WHERE first_name IS NULL OR TRIM(first_name) = ''
           OR last_name IS NULL OR TRIM(last_name) = ''
           OR email IS NULL OR TRIM(email) = ''
    """,
    "dq_valid_dates": f"""
        SELECT COUNT(*) = 0
        FROM staging_users
        WHERE dob_date IS NULL OR registered_date IS NULL
           OR dob_date !~ '{DATE_PATTERN}'
           OR registered_date !~ '{DATE_PATTERN}'
    """,
}
