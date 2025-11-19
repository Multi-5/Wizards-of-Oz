import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta

START_DATE = pendulum.datetime(2024, 1, 1, tz="UTC")

# Use container paths: project `./data` is mounted to `/opt/airflow/data` in the container
SRC_JSON = "/opt/airflow/data/FoodData_Central_foundation_food_json_2025-04-24.json"
DST_JSON = "/opt/airflow/data/fooddata_USA.json"
SRC_PARQUET = "/opt/airflow/data/product-database/food.parquet"
DST_PARQUET = "/opt/airflow/data/big_food.parquet"

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

    get_spreadsheet_1 = BashOperator(
        task_id="get_spreadsheet_1",
        bash_command=f"cp {SRC_JSON} {DST_JSON} || true",
    )

    get_spreadsheet_2 = BashOperator(
        task_id="get_spreadsheet_2",
        bash_command=f"cp {SRC_PARQUET} {DST_PARQUET} || true",
    )

    transmute_to_csv = EmptyOperator(task_id="transmute_to_csv")
    time_filter = EmptyOperator(task_id="time_filter")
    load = EmptyOperator(task_id="load")
    cleanup = EmptyOperator(task_id="cleanup")

    get_spreadsheet_1 >> get_spreadsheet_2 >> transmute_to_csv >> time_filter >> load >> cleanup