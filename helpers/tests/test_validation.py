"""Run with: python -m unittest discover -s /opt/airflow/helpers/tests."""
import unittest
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from airflow.exceptions import AirflowException
from airflow.sdk import DAG
from utilities.validation import BigQueryDataValidationOperator


def result_row(diff="0", source="100", output="100", name="totals"):
    return dict(test_name=name,
                output_value=Decimal(output) if output is not None else None,
                source_value=Decimal(source) if source is not None else None,
                diff=Decimal(diff) if diff is not None else None)


class ValidationTests(unittest.TestCase):
    def run_check(self, rows, schema=None, **kwargs):
        class Results(list):
            pass
        results = Results(rows)
        results.schema = schema if schema is not None else [
            SimpleNamespace(name=name, field_type=kind, mode="NULLABLE")
            for name, kind in [("test_name", "STRING"), ("output_value", "NUMERIC"),
                               ("source_value", "NUMERIC"), ("diff", "NUMERIC")]]
        with patch("utilities.validation.BigQueryHook") as hook:
            client = hook.return_value.get_client.return_value
            client.query.return_value.result.return_value = results
            operator = BigQueryDataValidationOperator(task_id="check", sql="SELECT ...", **kwargs)
            operator.execute({})
            hook.assert_called_once_with(gcp_conn_id="google_cloud_default",
                                         location=operator.location, use_legacy_sql=False)
            client.query.assert_called_once_with("SELECT ...", location=operator.location)

    def test_pass_and_inclusive_boundaries(self):
        self.run_check([result_row(value) for value in
                        ["0", "0.000000099", "0.0000001", "-0.0000001"]])

    def test_failure_preserves_sign_and_precision(self):
        with self.assertRaises(AirflowException) as caught:
            self.run_check([result_row("-0.033333")])
        self.assertEqual(str(caught.exception), "Data check for totals failed by -0.033333%.")

    def test_multiple_failures(self):
        with self.assertRaises(AirflowException) as caught:
            self.run_check([result_row("0.000000101", name="first"),
                            result_row("0"), result_row("-1", name="last")])
        self.assertEqual(str(caught.exception),
                         "Data check for first failed by 1.01E-7%.\n"
                         "Data check for last failed by -1%.")

    def test_both_zero(self):
        self.run_check([result_row(None, source="0", output="0"),
                        result_row("0", source="0", output="0")])

    def test_zero_source(self):
        with self.assertRaisesRegex(AirflowException, "percentage difference is undefined"):
            self.run_check([result_row(None, source="0", output="1")])

    def test_nulls(self):
        for column in ["test_name", "output_value", "source_value", "diff"]:
            with self.subTest(column=column):
                row = result_row()
                row[column] = None
                with self.assertRaisesRegex(AirflowException, "NULL"):
                    self.run_check([row])

    def test_empty(self):
        with self.assertRaisesRegex(AirflowException, "no results"):
            self.run_check([])

    def test_invalid_schema(self):
        for schema in [[], [SimpleNamespace(name="unexpected", field_type="STRING")]]:
            with self.subTest(schema=schema):
                with self.assertRaisesRegex(AirflowException, "exactly"):
                    self.run_check([result_row()], schema=schema)

    def test_invalid_tolerance(self):
        for tolerance in ["invalid", "-1", "NaN", "Infinity", None, True]:
            with self.subTest(tolerance=tolerance):
                with self.assertRaisesRegex(AirflowException, "Tolerance"):
                    BigQueryDataValidationOperator(task_id="check", sql="SELECT 1",
                                                   tolerance=tolerance)

    def test_custom_tolerance(self):
        self.run_check([result_row("-0.5")], tolerance=Decimal("0.5"))

    def test_sql_file_templating(self):
        with TemporaryDirectory() as directory:
            Path(directory, "check.sql").write_text("SELECT '{{ params.metric }}'", encoding="utf-8")
            with DAG(dag_id="validation_test", template_searchpath=[directory],
                     params={"metric": "revenue"}) as dag:
                operator = BigQueryDataValidationOperator(task_id="check", sql="check.sql")
            operator.render_template_fields({"params": dag.params})
            self.assertEqual(operator.sql, "SELECT 'revenue'")


if __name__ == "__main__":
    unittest.main()
