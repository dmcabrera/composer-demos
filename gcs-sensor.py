from airflow.contrib.operators.gcs_list_operator import GoogleCloudStorageListOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStoragePrefixSensor
import datetime as dt
from airflow.models import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator

lasthour = dt.datetime.now() - dt.timedelta(hours=1)

args = {
    'owner': 'airflow',
    'start_date': lasthour,
    'depends_on_past': False,
}
dag = DAG(
    dag_id='GCS_sensor_dag_v1.3',
    schedule_interval=None,
    default_args=args
)
GCS_File_list = GoogleCloudStorageListOperator(
    task_id='list_Files',
    bucket='wom_composer_demos',
    prefix='data/',
    delimiter='.csv',
    google_cloud_storage_conn_id='google_cloud_default',
    dag=dag
)
file_sensor = GoogleCloudStoragePrefixSensor(
    task_id='gcs_polling',
    bucket='wom_composer_demos',
    prefix='data',
    dag=dag
)

trigger = TriggerDagRunOperator(
    task_id='trigger_dag_{timestamp}_rerun'.format(timestamp=(
        (dt.datetime.now() - dt.datetime.utcfromtimestamp(0)).total_seconds()*1000)),
    trigger_dag_id="GCS_sensor_dag_v1.3",
    dag=dag
)

file_sensor >> GCS_File_list >> trigger
