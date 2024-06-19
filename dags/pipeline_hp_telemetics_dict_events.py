import logging
import time
import pytz
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from requests import get, post
import json
import hashlib
import pandas as pd
from google.cloud import storage
from io import BytesIO
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime, timedelta, timezone


# Variables
last_call_timestamp = None
hp_mix_url_identity = Variable.get("hp_mix_url_identity")
hp_mix_username_access_identity = Variable.get("hp_mix_username_access_identity")
hp_mix_password_access_identity = Variable.get("hp_mix_password_access_identity")
hp_mix_auth_token_basic_identity = Variable.get("hp_mix_auth_token_basic_identity")
hp_mix_group_id = Variable.get("hp_mix_group_id")
hp_gcp_bucket_name_raw = Variable.get("hp_gcp_bucket_name_raw")
token_data = {
    "token": None,
    "timestamp": None
}

def get_token_bearer():
    global token_data
    if token_data["token"] is not None and (datetime.now() - token_data["timestamp"]) < timedelta(seconds=3600):
        return token_data["token"]
    else:
        url = f"{hp_mix_url_identity}"
        payload = {
            'grant_type': 'password',
            'username': f'{hp_mix_username_access_identity}',
            'password': f'{hp_mix_password_access_identity}',
            'scope': 'offline_access MiX.Integrate'
        }
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': f'Basic {hp_mix_auth_token_basic_identity}'
        }
        response = post(url, headers=headers, data=payload)
        if response.status_code == 200:
            data = response.json()
            token_data["token"] = data['access_token']
            token_data["timestamp"] = datetime.now()
            return data['access_token']
        else:
            raise Exception(f"Failed to get token: HTTP {response.status_code}")


def generate_hash(*args):
    hash_input = "".join(args)
    return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()


def get_dict_events(**kwargs):
    global last_call_timestamp
    min_interval = 60 / 7  # limite imposto de 7 requisições por minuto segundo documentação
    if last_call_timestamp is not None:
        elapsed_time = time.time() - last_call_timestamp
        if elapsed_time < min_interval:
            time_to_wait = min_interval - elapsed_time
            time.sleep(time_to_wait)
    last_call_timestamp = time.time()

    # Ajustar o execution_date para o timezone correto
    execution_date = kwargs['logical_date'].replace(tzinfo=timezone.utc)
    execution_date = execution_date.astimezone(pytz.timezone('America/Sao_Paulo'))

    logging.info(f"Logical execution date: {kwargs['logical_date']}")
    logging.info(f"Adjusted execution date for data fetch: {execution_date}")

    formatted_date = execution_date.strftime("%Y%m%d")
    logging.info(f"Formatted execution date: {formatted_date}")

    url_stream = f"https://integrate.us.mixtelematics.com/api/libraryevents/organisation/{hp_mix_group_id}"
    headers = {"Authorization": f"Bearer {get_token_bearer()}"}
    response = get(url_stream, headers=headers)
    if response.status_code == 200:
        return response.text
    else:
        raise Exception(f"Failed to get geodata: HTTP {response.status_code}")

def transmission_gcp(**kwargs):
    #Recebendo dados do XCOM e convertendo para dataframe
    dict_events_json = kwargs['ti'].xcom_pull(task_ids='get_dict_events')
    data = json.loads(dict_events_json)
    df_dict_events = pd.DataFrame.from_dict(data)
    #Transmitindo os dados por BytesIO
    buffer = BytesIO()
    df_dict_events.to_parquet(buffer, index=False)
    buffer.seek(0)
    #Enviando para GCP
    gcs_hook = GCSHook(gcp_conn_id='gcp')
    gcs_hook.upload(
        bucket_name=hp_gcp_bucket_name_raw,
        object_name=f"mix/dict_events/dict_events.parquet",
        data=buffer.getvalue(),
        mime_type='application/octet-stream'
    )

    logging.info(f"Arquivo Parquet enviado para o bucket GCP: dict_events.parquet")


def insert_dag_metadata(**kwargs):
    execution_date = kwargs['logical_date'].replace(tzinfo=timezone.utc)
    execution_date = execution_date.astimezone(pytz.timezone('America/Sao_Paulo')) - timedelta(days=1)
    ti = kwargs['ti']
    start_time = ti.xcom_pull(key='start_time', task_ids='mark_start')
    end_time = ti.xcom_pull(key='end_time', task_ids='mark_end')

    if not start_time or not end_time:
        error_msg = f"Start time or end time not set correctly. Start: {start_time}, End: {end_time}"
        print(error_msg)
        raise ValueError(error_msg)

    duration = (end_time - start_time).total_seconds()

    dag_id = kwargs['dag_run'].dag_id
    execution_date = kwargs['ds']

    metadata = {
        "dag_id": dag_id,
        "execution_date": execution_date,
        "start_date": str(start_time),
        "end_date": str(end_time),
        "duration": duration,
        "success": True,
        "error_message": None
    }

    # Salva os metadados em formato JSON no buffer
    metadata_buffer = BytesIO()
    metadata_buffer.write(json.dumps(metadata).encode('utf-8'))
    metadata_buffer.seek(0)

    # Envia o arquivo JSON para o bucket GCP usando GCSHook
    gcs_hook = GCSHook(gcp_conn_id='gcp')
    gcs_hook.upload(
        bucket_name=hp_gcp_bucket_name_raw,
        object_name=f"mix/metadata/dict_events/pipeline_hp_telemetics_events_{execution_date}.json",
        data=metadata_buffer.getvalue(),
        mime_type='application/json'
    )

    logging.info(
        f"Arquivo JSON de metadados enviado para o bucket GCP: pipeline_hp_telemetics_events_{execution_date}.json")


def mark_start(**context):
    start = datetime.now()
    context['ti'].xcom_push(key='start_time', value=start)
    print(f"Mark start at {start}")


def mark_end(**context):
    end = datetime.now()
    context['ti'].xcom_push(key='end_time', value=end)
    print(f"Mark end at {end}")


@dag(start_date=datetime(2024, 6, 18),
     schedule='30 11 * * *',
     catchup=True,
     tags=['airbyte', 'HP', 'Mix-Telematics'])
def pipeline_hp_mix_telemetics_dict_events():
    start = EmptyOperator(task_id='start')

    start_task = PythonOperator(
        task_id='mark_start',
        python_callable=mark_start,
        provide_context=True,
    )

    get_bearer_token = PythonOperator(
        task_id='get_bearer_token',
        python_callable=get_token_bearer,
        retries=5,
        retry_delay=timedelta(minutes=5)
    )

    get_data = PythonOperator(
        task_id='get_dict_events',
        python_callable=get_dict_events,
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=1)
    )

    transmission_data = PythonOperator(
        task_id='transmission_gcp',
        python_callable=transmission_gcp,
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=5)
    )

    create_metadata = PythonOperator(
        task_id='insert_dag_metadata',
        python_callable=insert_dag_metadata,
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=5)
    )
    end_task = PythonOperator(
        task_id='mark_end',
        python_callable=mark_end,
        provide_context=True,
    )
    end = EmptyOperator(task_id='end')

    start >> start_task >> get_bearer_token >> get_data >> transmission_data >> end_task >> create_metadata >> end


dag = pipeline_hp_mix_telemetics_dict_events()