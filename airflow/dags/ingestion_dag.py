import os
import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta

START_DATE = pendulum.datetime(2024, 1, 1, tz="UTC")


# Neue Pfade: Rohdaten liegen jetzt in /opt/airflow/data/data_unclean/
SRC_JSON = "/opt/airflow/data/data_unclean/FoodData_Central_foundation_food_json_2025-04-24.json"
SRC_PARQUET = "/opt/airflow/data/data_unclean/food.parquet"
# Ziel: Landing Zone
DST_JSON = "/opt/airflow/data/landing/FoodData_Central_foundation_food_json_2025-04-24.json"
DST_PARQUET = "/opt/airflow/data/landing/food.parquet"


def _ensure_directories(paths: set[str]) -> None:
    for path in paths:
        if not path:
            continue
        os.makedirs(path, exist_ok=True)


_ensure_directories({
    os.path.dirname(SRC_JSON),
    os.path.dirname(SRC_PARQUET),
    os.path.dirname(DST_JSON),
    os.path.dirname(DST_PARQUET),
})

## If the files are not present in /opt/airflow/data inside the container, ensure docker-compose mounts the host ./data folder to /opt/airflow/data in the container.
## If the host data is outside the airflow folder, update the docker-compose.yml volumes section accordingly.
with DAG(
    dag_id="ingestion_dag",
    start_date=START_DATE,
    schedule=None,  # no schedule
    catchup=False,
    max_active_tasks=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["stub"],
) as dag:


    ingest_json = BashOperator(
        task_id="ingest_json",
        bash_command=f"cp {SRC_JSON} {DST_JSON} || true",
    )

    ingest_parquet = BashOperator(
        task_id="ingest_parquet",
        bash_command=f"cp {SRC_PARQUET} {DST_PARQUET} || true",
    )

    ingest_json >> ingest_parquet