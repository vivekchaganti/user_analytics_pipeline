import unittest
from airflow.models import DagBag


class TestDagIntegrity(unittest.TestCase):
    def setUp(self):
        self.dagbag = DagBag(
            dag_folder="/Users/vivekchaganti/projects/user_analytics_pipeline/dags",
            include_examples=False,
        )

    def test_dag_loaded(self):
        """Test that the DAG is correctly loaded."""
        self.assertFalse(self.dagbag.import_errors)
        self.assertIn("user_analytics_etl_pipeline", self.dagbag.dags)

    def test_task_count(self):
        """Test that the DAG has the expected number of tasks."""
        dag = self.dagbag.get_dag("user_analytics_etl_pipeline")
        self.assertEqual(len(dag.tasks), 4)

    def test_task_ids(self):
        """Test that all expected task IDs are present."""
        dag = self.dagbag.get_dag("user_analytics_etl_pipeline")
        expected_tasks = {
            "extract_api_to_parquet",
            "load_parquet_to_staging",
            "transform_staging_to_warehouse",
            "cleanup_old_parquet_files",
        }
        self.assertSetEqual(set(task.task_id for task in dag.tasks), expected_tasks)


if __name__ == "__main__":
    unittest.main()
