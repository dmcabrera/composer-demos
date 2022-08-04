"""
Example DAG using GCSToBigQueryOperator.
"""

import os

from airflow import models
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

DATASET_NAME = os.environ.get("GCP_DATASET_NAME", 'composer_demo_dataset')
TABLE_NAME = os.environ.get("GCP_TABLE_NAME", 'clientes2')

dag = models.DAG(
    dag_id='example_gcs_to_bigquery_operator',
    start_date=days_ago(2),
    schedule_interval=None,
    tags=['example'],
)

create_test_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_airflow_test_dataset', dataset_id=DATASET_NAME, dag=dag
)

# [START howto_operator_gcs_to_bigquery]
load_csv = GCSToBigQueryOperator(
    task_id='gcs_to_bigquery_example',
    bucket='wom_composer_demos',
    source_objects=['clientes.csv'],
    destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
    schema_fields=[
        {'name': 'RUT', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'NombreCompleto', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'InstitucionFinanciera', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'DeudaVigente', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Mora30_89', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Mora90_mas', 'type': 'STRING', 'mode': 'NULLABLE'}
    ],
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)
# [END howto_operator_gcs_to_bigquery]

delete_test_dataset = BigQueryDeleteDatasetOperator(
    task_id='delete_airflow_test_dataset', dataset_id=DATASET_NAME, delete_contents=True, dag=dag
)

create_test_dataset >> load_csv #>> delete_test_dataset