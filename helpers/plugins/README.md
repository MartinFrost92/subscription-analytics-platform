# Shared Airflow operators

The container adds this directory to `PYTHONPATH`. Import shared operators
from the `subscription_operators` package; no plugin registration is required.

## Waiting for upstream DAGs

`WaitForDagSensor` accepts `task_id`, `external_dag_id`, and `time_delta`.
Keep the two dependency settings in each DAG's config:

```yaml
wait_for_dags:
  - external_dag_id: transform_blabla_something
    time_delta: {hours: 1, minutes: 30}
  - external_dag_id: transform_another_example
    time_delta: {hours: 0, minutes: 0}
```

Loop through the entries to create one sensor per upstream DAG. All sensors
must succeed before downstream table creation. Use `wait_for_dags: []` when
the DAG has no upstream dependency.

The shared sensor checks whole-DAG success every 60 seconds, releases its
worker slot between checks, fails if the matching upstream run fails, and
times out after 15 minutes. Missing DAGs continue waiting until timeout.
No triggerer is needed.

Offsets subtract from the current run's **logical date**, not wall-clock time.
An offset of 1 hour 30 minutes targets the upstream logical date exactly
90 minutes earlier. It does not choose the latest success in that time window.
Schedules/logical dates must align; manual test runs need an appropriate
logical date. Fictional upstream names are placeholders and cannot satisfy
the dependency.
