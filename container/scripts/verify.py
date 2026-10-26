"""Verify scheduler execution inside the scheduler container.

Metadata reads are development checks, never pipeline implementation.
"""
import subprocess
import time
import uuid

from airflow.configuration import conf
from sqlalchemy import create_engine, text


def main() -> None:
    assert conf.get("core", "executor") == "LocalExecutor", "Expected LocalExecutor"
    engine = create_engine(conf.get("database", "sql_alchemy_conn"))
    dag_ids = ("staging_dag", "transform_dag", "model_dag")
    deadline = time.monotonic() + 180
    while True:
        with engine.connect() as conn:
            errors = conn.execute(text("SELECT filename, stacktrace FROM import_error")).all()
            if errors:
                raise RuntimeError(f"DAG import errors: {errors}")
            discovered = set(conn.execute(text("SELECT dag_id FROM dag")).scalars())
        if set(dag_ids).issubset(discovered):
            break
        if time.monotonic() > deadline:
            raise TimeoutError(f"DAG discovery timed out; found {discovered}")
        time.sleep(5)
    print(f"Discovered all three DAGs: {', '.join(dag_ids)}", flush=True)
    run_id = f"smoke_{uuid.uuid4().hex}"
    for dag_id in dag_ids:
        for args in (["unpause", dag_id], ["trigger", dag_id, "--run-id", run_id]):
            result = subprocess.run(["airflow", "dags", *args], capture_output=True, text=True)
            if result.returncode:
                raise RuntimeError(f"CLI failed: {result.stdout}\n{result.stderr}")
    deadline = time.monotonic() + 240
    while True:
        with engine.connect() as conn:
            runs = conn.execute(text("SELECT dag_id, state FROM dag_run WHERE run_id = :run_id"), {"run_id": run_id}).all()
            tasks = conn.execute(text("SELECT dag_id, task_id, state FROM task_instance WHERE run_id = :run_id"), {"run_id": run_id}).all()
        if any(state == "failed" for _, state in runs) or any(state in {"failed", "upstream_failed"} for _, _, state in tasks):
            raise RuntimeError(f"Smoke execution failed: runs={runs}, tasks={tasks}")
        if len(runs) == 3 and len(tasks) == 3 and all(state == "success" for _, state in runs) and all(state == "success" for _, _, state in tasks):
            for dag_id, task_id, state in tasks:
                print(f"PASS {dag_id}.{task_id}: {state} ({run_id})", flush=True)
            break
        if time.monotonic() > deadline:
            raise TimeoutError(f"Execution timed out: runs={runs}, tasks={tasks}")
        time.sleep(5)
    engine.dispose()


if __name__ == "__main__":
    main()
