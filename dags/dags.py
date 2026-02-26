from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

from extract import extract
from transform import transform
from load import load

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="etl_pipeline",
    default_args=default_args,
    description="ETL pipeline: Extract, Transform, Load",
    schedule_interval=None,
    catchup=False,
    tags=["etl"],
) as dag:

    start = DummyOperator(task_id="start")

    extract = PythonOperator(
        task_id="extract_task",
        python_callable=extract,
    )

    transform = PythonOperator(
        task_id="transform_task",
        python_callable=transform,
    )

    load = PythonOperator(
        task_id="load_task",
        python_callable=load,
    )

    end = DummyOperator(task_id="end")

    start >> extract >> transform >> load >> end
