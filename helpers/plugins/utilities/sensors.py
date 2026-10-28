"""Shared defaults for waiting on upstream DAG runs."""
from datetime import timedelta

from airflow.providers.standard.sensors.external_task import ExternalTaskSensor


class WaitForDagSensor(ExternalTaskSensor):
    """Wait up to 15 minutes for an upstream DAG's matching run to succeed.

    Positive time_delta values look back from this run's logical date,
    not from the current wall-clock time.
    """

    def __init__(self, *, external_dag_id: str, time_delta: dict | None = None, **kwargs):
        time_delta = time_delta or {}
        self.time_delta = time_delta
        execution_delta = timedelta(
            hours=time_delta.get("hours", 0),
            minutes=time_delta.get("minutes", 0),
        )
        if execution_delta < timedelta(0):
            raise ValueError("time_delta must look back by a nonnegative duration")

        defaults = {
            "external_task_id": None,
            "allowed_states": ["success"],
            "failed_states": ["failed"],
            "check_existence": False,
            "mode": "reschedule",
            "poke_interval": 60,
            "timeout": 15 * 60,
            "deferrable": False,
        }
        defaults.update(kwargs)
        super().__init__(
            external_dag_id=external_dag_id,
            execution_delta=execution_delta,
            **defaults,
        )
