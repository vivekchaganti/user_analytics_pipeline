import os
import unittest

import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag  # noqa: E402


class TestDagIntegrity(unittest.TestCase):
    def setUp(self):
        dag_folder = os.path.join(os.path.dirname(__file__), "..", "dags")
        self.dagbag = DagBag(dag_folder=dag_folder, include_examples=False)

    def test_dag_loaded(self):
        """Test that the DAG is correctly loaded."""
        self.assertFalse(self.dagbag.import_errors)
        self.assertIn("user_analytics_etl_pipeline", self.dagbag.dags)

    def test_task_count(self):
        """Test that the DAG has the expected number of tasks."""
        dag = self.dagbag.get_dag("user_analytics_etl_pipeline")
        self.assertEqual(len(dag.tasks), 13)

    def test_task_ids(self):
        """Test that all expected task IDs are present."""
        dag = self.dagbag.get_dag("user_analytics_etl_pipeline")
        expected_tasks = {
            "extract_api_to_parquet",
            "load_parquet_to_staging",
            "dq_staging_not_empty",
            "dq_no_null_uuid",
            "dq_no_duplicate_uuid",
            "dq_required_fields_present",
            "dq_valid_dates",
            "ensure_warehouse_schema",
            "update_dim_users",
            "update_dim_locations",
            "update_dim_dates",
            "update_fact_user_registrations",
            "cleanup_old_parquet_files",
        }
        self.assertSetEqual(set(task.task_id for task in dag.tasks), expected_tasks)


if __name__ == "__main__":
    unittest.main()
