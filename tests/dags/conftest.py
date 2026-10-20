import pytest


@pytest.fixture(scope="session")
def dagbag():
    """DagBag of the mounted DAGs folder (runs inside the Airflow containers only).

    Airflow 3.3's DagBag has no `include_examples` argument; example DAGs are excluded by
    AIRFLOW__CORE__LOAD_EXAMPLES=False in the compose environment.
    """
    from airflow.dag_processing.dagbag import DagBag

    return DagBag(dag_folder="/opt/airflow/dags")
