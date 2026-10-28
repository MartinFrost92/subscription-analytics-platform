"""Shared naming for DAG files and copyable DAG templates."""
from pathlib import Path


def get_dag_id(file_path):
    """Join the path below dags/ (or dag_templates/) without its extension."""
    dag_file = Path(file_path).resolve()
    dag_root = next(
        parent for parent in dag_file.parents
        if parent.name in ('dags', 'dag_templates')
    )
    return '_'.join(dag_file.relative_to(dag_root).with_suffix('').parts)
