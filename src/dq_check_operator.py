from airflow.exceptions import AirflowException
from airflow.providers.common.sql.operators.sql import SQLValueCheckOperator

_TRUTHY = frozenset({True, "true", "t", "1", 1})


class DQSqlCheckOperator(SQLValueCheckOperator):
    """Run SQL that returns TRUE/FALSE and fail the task when the result is not TRUE."""

    template_fields = (*SQLValueCheckOperator.template_fields,)

    def __init__(self, *, sql, conn_id, task_id, pass_value=True, **kwargs):
        super().__init__(
            task_id=task_id,
            conn_id=conn_id,
            sql=sql,
            pass_value=pass_value,
            **kwargs,
        )

    def execute(self, context):
        hook = self.get_db_hook()
        record = hook.get_first(self.sql)

        if record is None:
            raise AirflowException(
                f"DQ check '{self.task_id}' returned no rows; expected TRUE or FALSE"
            )

        result = record[0]
        if result in _TRUTHY:
            return True

        raise AirflowException(
            f"DQ check '{self.task_id}' failed: SQL returned {result!r}, expected TRUE"
        )
