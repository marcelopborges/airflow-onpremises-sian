from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='dag_test_gcp',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    list_buckets = GCSListObjectsOperator(
        task_id='list_buckets',
        bucket='sian-projetos-raw',  # Substitua pelo nome do seu bucket
        gcp_conn_id='gcp'  # Substitua pelo ID da sua conexão configurada
    )

    list_buckets
