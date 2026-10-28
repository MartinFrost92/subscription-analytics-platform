"""Run the same SQL validation contract across all BigQuery layers."""
import os
from decimal import Decimal, InvalidOperation

from airflow.exceptions import AirflowException
from airflow.sdk import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


class BigQueryDataValidationOperator(BaseOperator):
    """Validate four-column SQL results; tolerance is in percentage points.

    SQL may be inline or a DAG-relative .sql file, with standard Airflow Jinja.
    Results must contain test_name, output_value, source_value, and diff.
    """

    template_fields = ("sql", "project_id", "location")
    template_ext = (".sql",)
    template_fields_renderers = {"sql": "sql"}

    def __init__(self, *, sql: str, tolerance="0.0000001",
                 gcp_conn_id="google_cloud_default", project_id=None,
                 location=None, **kwargs):
        super().__init__(**kwargs)
        try:
            tolerance = Decimal(str(tolerance))
        except (InvalidOperation, ValueError, TypeError) as error:
            raise AirflowException("Tolerance must be a finite, nonnegative number.") from error
        if not tolerance.is_finite() or tolerance < 0:
            raise AirflowException("Tolerance must be a finite, nonnegative number.")
        self.tolerance = tolerance
        self.sql = sql
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.location = location or os.environ.get("BIGQUERY_LOCATION", "EU")

    def execute(self, context):
        hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id,
                            location=self.location, use_legacy_sql=False)
        client = hook.get_client(project_id=self.project_id, location=self.location)
        results = client.query(self.sql, location=self.location).result()
        expected = {"test_name": "STRING", "output_value": "NUMERIC",
                    "source_value": "NUMERIC", "diff": "NUMERIC"}
        if (len(results.schema) != 4
                or {field.name: field.field_type for field in results.schema} != expected
                or any(field.mode == "REPEATED" for field in results.schema)):
            raise AirflowException(
                "Validation SQL must return exactly test_name STRING, "
                "output_value NUMERIC, source_value NUMERIC, and diff NUMERIC."
            )

        failures = []
        count = 0
        for row in results:
            count += 1
            test_name = row["test_name"]
            output_value = row["output_value"]
            source_value = row["source_value"]
            diff = row["diff"]
            if test_name is None or output_value is None or source_value is None:
                failures.append(f"Data check for {test_name} failed: NULL validation value.")
            elif source_value == 0 and output_value != 0:
                failures.append(
                    f"Data check for {test_name} failed: percentage difference is undefined "
                    "because source_value is zero and output_value is nonzero."
                )
            elif source_value == 0 and output_value == 0:
                # Both totals being zero defines a zero difference, even if SQL returned NULL.
                continue
            elif diff is None:
                failures.append(f"Data check for {test_name} failed: NULL percentage difference.")
            elif not isinstance(diff, Decimal) or not diff.is_finite():
                failures.append(f"Data check for {test_name} failed: diff must be a finite Decimal.")
            elif diff.copy_abs() > self.tolerance:
                failures.append(f"Data check for {test_name} failed by {diff}%.")
        if count == 0:
            raise AirflowException("Validation SQL returned no results.")
        if failures:
            raise AirflowException("\n".join(failures))
        self.log.info("All %s data checks passed.", count)
