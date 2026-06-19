from unittest.mock import patch

import pytest

pytest.importorskip("airflow")

from airflow.exceptions import AirflowException  # noqa: E402
from dq_check_operator import DQSqlCheckOperator  # noqa: E402


def test_dq_operator_passes_on_true():
    operator = DQSqlCheckOperator(
        task_id="dq_test",
        conn_id="user_analytics_postgres_conn",
        sql="SELECT TRUE",
    )
    with patch.object(operator, "get_db_hook") as mock_get_hook:
        mock_get_hook.return_value.get_first.return_value = (True,)
        assert operator.execute({}) is True


def test_dq_operator_fails_on_false():
    operator = DQSqlCheckOperator(
        task_id="dq_test",
        conn_id="user_analytics_postgres_conn",
        sql="SELECT FALSE",
    )
    with patch.object(operator, "get_db_hook") as mock_get_hook:
        mock_get_hook.return_value.get_first.return_value = (False,)
        with pytest.raises(AirflowException, match="failed: SQL returned False"):
            operator.execute({})


def test_dq_operator_fails_when_sql_returns_no_rows():
    operator = DQSqlCheckOperator(
        task_id="dq_test",
        conn_id="user_analytics_postgres_conn",
        sql="SELECT TRUE",
    )
    with patch.object(operator, "get_db_hook") as mock_get_hook:
        mock_get_hook.return_value.get_first.return_value = None
        with pytest.raises(AirflowException, match="returned no rows"):
            operator.execute({})
