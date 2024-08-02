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
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


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


def get_events(**kwargs):
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

    url_stream = f"https://integrate.us.mixtelematics.com/api/events/assets/from/{formatted_date}000000/to/{formatted_date}235959"
    entity_ids_str = Variable.get("hp_mix_list_car")
    event_type_ids_str = Variable.get('hp_mix_list_events')
    entity_ids = json.loads(entity_ids_str)
    event_type_ids = json.loads(event_type_ids_str)
    logging.info(f"EntityIds: {entity_ids}")
    logging.info(f"EventTypeIds: {event_type_ids}")
    event_filter = {
        "EntityIds": entity_ids,
        "EventTypeIds": event_type_ids,
        "MenuId": "string"
    }
    payload = json.dumps(event_filter)
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {get_token_bearer()}"
    }
    response = post(url_stream, headers=headers, data=payload)

    if response.status_code == 200:
        return response.text
    else:
        logging.info(f"Response Status Code: {response.status_code}")
        logging.info(f"Response Text: {response.text}")
        logging.info(f"Request Payload: {payload}")
        raise Exception(f"Failed to get geodata: HTTP {response.status_code}")



def transmission_gcp(**kwargs):
    execution_date = kwargs['logical_date'].replace(tzinfo=timezone.utc)
    execution_date = execution_date.astimezone(pytz.timezone('America/Sao_Paulo'))
    formatted_date = execution_date.strftime("%Y%m%d")
    events_json = kwargs['ti'].xcom_pull(task_ids='get_events')
    if not events_json:
        raise ValueError("Nenhum dado foi retornado pelo XCOM.")
    try:
        data = json.loads(events_json)
    except json.JSONDecodeError as e:
        raise ValueError(f"Erro ao decodificar JSON: {str(e)}")

    if not isinstance(data, list):
        raise ValueError("Os dados JSON não estão no formato esperado (lista).")

    # Convertendo para DataFrame
    df_events = pd.DataFrame.from_dict(data)
    #Transmitindo os dados por BytesIO
    buffer = BytesIO()
    df_events.to_parquet(buffer, index=False)
    buffer.seek(0)
    #Enviando para GCP
    gcs_hook = GCSHook(gcp_conn_id='gcp')
    gcs_hook.upload(
        bucket_name=hp_gcp_bucket_name_raw,
        object_name=f"mix/events/events_{formatted_date}.parquet",
        data=buffer.getvalue(),
        mime_type='application/octet-stream'
    )

    logging.info(f"Arquivo Parquet enviado para o bucket GCP: cars.parquet")

def insert_dag_metadata(**kwargs):
    execution_date = kwargs['logical_date'].replace(tzinfo=timezone.utc)
    execution_date = execution_date.astimezone(pytz.timezone('America/Sao_Paulo'))
    formatted_date = execution_date.strftime("%Y%m%d")
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
        object_name=f"mix/metadata/events/pipeline_hp_telemetics_events_{formatted_date}.json",
        data=metadata_buffer.getvalue(),
        mime_type='application/json'
    )

    logging.info(
        f"Arquivo JSON de metadados enviado para o bucket GCP: pipeline_hp_telemetics_events_{formatted_date}.json")
    
def atualizar_tabela_bigquery(**kwargs):
    execution_date = kwargs['logical_date'].replace(tzinfo=timezone.utc)
    execution_date = execution_date.astimezone(pytz.timezone('America/Sao_Paulo'))
    formatted_date = execution_date.strftime("%Y%m%d")

    gcs_to_bigquery = GCSToBigQueryOperator(
        task_id='atualizar_tabela_bigquery',
        bucket=hp_gcp_bucket_name_raw,
        source_objects=[f"mix/events/events_{formatted_date}.parquet"],
        destination_project_dataset_table='nome-do-projeto.nome-do-dataset.nome-da-tabela',
        source_format='PARQUET',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        gcp_conn_id='gcp'
    )

    return gcs_to_bigquery.execute(context=kwargs)

def mark_start(**context):
    start = datetime.now()
    context['ti'].xcom_push(key='start_time', value=start)
    print(f"Mark start at {start}")

def mark_end(**context):
    end = datetime.now()
    context['ti'].xcom_push(key='end_time', value=end)
    print(f"Mark end at {end}")


@dag(start_date=datetime(2024, 2, 26),
     schedule='35 9 * * *',
     catchup=False,
     tags=['airbyte', 'HP', 'Mix-Telematics'])
def pipeline_hp_mix_telemetics_events_copy():
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
        task_id='get_events',
        python_callable=get_events,
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
    atualizar_tabela_bigquery_task = PythonOperator(
    task_id='atualizar_tabela_bigquery',
    python_callable=atualizar_tabela_bigquery,
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

    start >> start_task >> get_bearer_token >> get_data >> transmission_data >> end_task >> create_metadata >> atualizar_tabela_bigquery_task >> end
    
    
dag = pipeline_hp_mix_telemetics_events_copy()


