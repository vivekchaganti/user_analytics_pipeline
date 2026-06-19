from dq_checks_sql import STAGING_DQ_CHECKS


def test_staging_dq_checks_are_defined():
    assert len(STAGING_DQ_CHECKS) == 5


def test_staging_dq_checks_return_boolean_expressions():
    for task_id, sql in STAGING_DQ_CHECKS.items():
        normalized = " ".join(sql.split())
        assert "SELECT" in normalized.upper(), task_id
        assert "FROM staging_users" in normalized, task_id
