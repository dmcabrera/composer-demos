import datetime

from airflow import models
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDatasetTablesOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator

project_id = models.Variable.get("project_id")

default_args = {
    "start_date": days_ago(1),
    "project_id": project_id
}

with models.DAG(
    "bigquery_demos_dag_v1.3",
    default_args=default_args,
    schedule_interval=datetime.timedelta(
        days=1),  # Override to match your needs
) as dag:
    DATASET_NAME = "composer_demo_dataset"
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", dataset_id=DATASET_NAME)

    dataset = BigQueryGetDatasetOperator(
        task_id="get-dataset", dataset_id=DATASET_NAME)

    dataset_tables = BigQueryGetDatasetTablesOperator(
        task_id="get_dataset_tables", dataset_id=DATASET_NAME
    )

    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id=DATASET_NAME,
        table_id="test_table",
        schema_fields=[
            {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
        ],
    )

    get_data = BigQueryGetDataOperator(
        task_id="get_data",
        dataset_id=DATASET_NAME,
        table_id="test_table",
        max_results=10,
        selected_fields="emp_name,salary",
        location="us-central1",
    )
