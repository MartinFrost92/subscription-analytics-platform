"""DAG IDs are relative to the DAG root, including copyable templates."""
from pathlib import Path
import runpy
import unittest

from utilities.dag import get_dag_id


class DagNamingTests(unittest.TestCase):
    def test_path_convention(self):
        root = Path(__file__).resolve().parents[2]
        for path, expected in [
            ('dags/staging/synthetic_subscription_api/import.py', 'staging_synthetic_subscription_api_import'),
            ('dags/transform/synthetic_api/subscriptions.py', 'transform_synthetic_api_subscriptions'),
            ('helpers/dag_templates/transform_model/model_dag.py', 'transform_model_model_dag'),
            ('helpers/dag_templates/staging_rest_api/import.py', 'staging_rest_api_import'),
        ]:
            with self.subTest(path=path):
                self.assertEqual(get_dag_id(root / path), expected)
                self.assertEqual(runpy.run_path(str(root / path))['dag'].dag_id, expected)

    def test_upstream_reference(self):
        root = Path(__file__).resolve().parents[2]
        staging = runpy.run_path(str(root / 'dags/staging/synthetic_subscription_api/import.py'))['dag']
        transform = runpy.run_path(str(root / 'dags/transform/synthetic_api/subscriptions.py'))['dag']
        sensor = transform.get_task(f'wait_for_{staging.dag_id}')
        self.assertEqual(sensor.external_dag_id, staging.dag_id)
