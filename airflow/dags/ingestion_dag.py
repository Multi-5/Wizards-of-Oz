import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

START_DATE = pendulum.datetime(2024, 1, 1, tz="UTC")

with DAG(
    dag_id="ingestion_dag",
    start_date=START_DATE,
    schedule=None,          # no schedule
    catchup=False,
    max_active_tasks=1,     # old 'concurrency'
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["stub"],
) as dag:

    get_spreadsheet_1 = EmptyOperator(task_id="get_spreadsheet")
    get_spreadsheet_2 = EmptyOperator(task_id="get_spreadsheet")

    transmute_to_csv = EmptyOperator(task_id="transmute_to_csv")
    time_filter = EmptyOperator(task_id="time_filter")
    load = EmptyOperator(task_id="load")
    cleanup = EmptyOperator(task_id="cleanup")

    get_spreadsheet_1 >> get_spreadsheet_2 >> transmute_to_csv >> time_filter >> load >> cleanup