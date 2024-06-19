import hashlib
import json
import logging
from datetime import datetime, timedelta
import requests
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from requests import get, post
from io import BytesIO
import pandas as pd
from airflow.providers.google.cloud.hooks.gcs import GCSHook

last_call_timestamp = None
hp_sianet_url_api = Variable.get("hp_sianet_url_api")
hp_sianet_auth_token_basic = Variable.get("hp_sianet_auth_token_basic")
hp_gcp_bucket_name_raw = Variable.get("hp_gcp_bucket_name_raw")

token_data = {
    "token": None,
    "timestamp": None
}


def generate_hash(*args):
    hash_input = "".join(args)
    return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()


def convert_datetime_with_time(date_str):
    if date_str is None:
        return None
    try:
        return datetime.strptime(date_str, '%d/%m/%Y %H:%M').strftime('%Y-%m-%d %H:%M')
    except ValueError:
        logging.error(f"Data e hora fornecida inválida: {date_str}")
        return None


def convert_datetime_without_time(date_str):
    if date_str is None:
        return None
    try:
        return datetime.strptime(date_str, '%d/%m/%Y').strftime('%Y-%m-%d')
    except ValueError:
        logging.error(f"Data fornecida inválida: {date_str}")
        return None


def get_token_sianet():
    global token_data
    if token_data["token"] is not None and (datetime.now() - token_data["timestamp"]) < timedelta(seconds=1800):
        return token_data["token"]
    else:
        url = f"{hp_sianet_url_api}"
        payload = {}
        headers = {
            'Authorization': f'Basic {hp_sianet_auth_token_basic}'
        }
        response = post(url, headers=headers, data=payload)
        if response.status_code == 200:
            data = response.json()
            token_data["token"] = data['token']
            token_data["timestamp"] = datetime.now()
            return data['token']
        else:
            raise Exception(f"Failed to get token: HTTP {response.status_code}")


def get_data_trip_made_sianet(**kwargs):
    logical_date = kwargs['logical_date']
    logging.info(f"Original logical_date: {logical_date}")

    logical_date = logical_date
    logging.info(f"Adjusted logical_date (D-1): {logical_date}")

    formatted_date = logical_date.strftime("%d/%m/%Y")
    logging.info(f"Formatted date for API request: {formatted_date}")

    url = f"http://siannet.gestaosian.com/api/ConsultaViagens?data={formatted_date}&viagens=programadas"
    headers = {"Authorization": f"Bearer {get_token_sianet()}"}

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = json.loads(response.text)
        programadas_data = []
        for key, value in data.get('dados', {}).get('programadas', {}).items():
            programadas_data.extend(value)
        process_and_load_trip_data_to_gcp(programadas_data, logical_date)
    else:
        raise Exception(f"Failed to get data: HTTP {response.status_code}")


def process_and_load_trip_data_to_gcp(data, logical_date):
    logging.info(f"Processing data for logical_date (D-1): {logical_date}")

    rows_to_insert = []
    for entry in data:
        try:
            row = {
                "data": convert_datetime_without_time(entry.get('DATA')),
                "linha": entry.get('LINHA'),
                "carro": entry.get('CARRO'),
                "re": entry.get('RE'),
                "nome": entry.get('NOME'),
                "dthr_saida": convert_datetime_with_time(entry.get('DTHR_SAIDA')) if entry.get('DTHR_SAIDA') else None,
                "dthr_retorno": convert_datetime_with_time(entry.get('DTHR_RETORNO')) if entry.get('DTHR_RETORNO') else None,
                "dthr_chegada": convert_datetime_with_time(entry.get('DTHR_CHEGADA')) if entry.get('DTHR_CHEGADA') else None
            }
            rows_to_insert.append(row)
        except Exception as e:
            logging.error(f"Error processing row {entry}: {e}")

    df = pd.DataFrame(rows_to_insert)

    # Salva o DataFrame em formato Parquet no buffer
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    # Envia o arquivo Parquet diretamente para o bucket GCP usando GCSHook
    gcs_hook = GCSHook(gcp_conn_id='gcp')
    gcs_hook.upload(
        bucket_name=hp_gcp_bucket_name_raw,
        object_name=f"sianet/viagens_programadas/viagens_programadas_{logical_date.strftime('%Y-%m-%d')}.parquet",
        data=buffer.getvalue(),
        mime_type='application/octet-stream'
    )

    logging.info(f"Arquivo Parquet enviado para o bucket GCP: sianet/viagens_programadas/viagens_programadas_{logical_date.strftime('%Y-%m-%d')}.parquet")


def insert_dag_metadata_schedule(**kwargs):
    ti = kwargs['ti']
    start_time = ti.xcom_pull(key='start_time', task_ids='mark_start')
    end_time = ti.xcom_pull(key='end_time', task_ids='mark_end')

    if not start_time or not end_time:
        error_msg = f"Start time or end time not set correctly. Start: {start_time}, End: {end_time}"
        print(error_msg)
        raise ValueError(error_msg)

    duration = (end_time - start_time).total_seconds()

    metadata = {
        "dag_id": kwargs['dag_run'].dag_id,
        "execution_date": kwargs['ds'],
        "start_date": str(start_time),
        "end_date": str(end_time),
        "duration": duration,
        "success": True,
        "error_message": None
    }

    metadata_buffer = BytesIO()
    metadata_buffer.write(json.dumps(metadata).encode('utf-8'))
    metadata_buffer.seek(0)

    gcs_hook = GCSHook(gcp_conn_id='gcp')
    gcs_hook.upload(
        bucket_name=hp_gcp_bucket_name_raw,
        object_name=f"sianet/metadata/pipeline_hp_sianet_{kwargs['ds']}.json",
        data=metadata_buffer.getvalue(),
        mime_type='application/json'
    )

    logging.info(
        f"Arquivo JSON de metadados enviado para o bucket GCP: sianet/metadata/pipeline_hp_sianet_{kwargs['ds']}.json")


def mark_start(**context):
    start = datetime.now()
    context['ti'].xcom_push(key='start_time', value=start)
    print(f"Mark start at {start}")


def mark_end(**context):
    end = datetime.now()
    context['ti'].xcom_push(key='end_time', value=end)
    print(f"Mark end at {end}")


@dag(
    start_date=datetime(2024, 2, 26),
    schedule_interval='30 11 * * *',
    catchup=True,
    max_active_runs=1,
    tags=['airbyte', 'HP', 'Mix-Telematics']
)
def pipeline_hp_sianet_scheduler():
    start = EmptyOperator(task_id='start')
    start_task = PythonOperator(
        task_id='mark_start',
        python_callable=mark_start,
        provide_context=True,
        retries=5,
        retry_delay=timedelta(minutes=5)
    )
    get_token = PythonOperator(
        task_id='get_token',
        python_callable=get_token_sianet,
        provide_context=True,
        retries=5,
        retry_delay=timedelta(minutes=5)
    )
    get_data_line = PythonOperator(
        task_id='get_data_line',
        python_callable=get_data_trip_made_sianet,
        provide_context=True,
        retries=5,
        retry_delay=timedelta(minutes=5)
    )
    end_task = PythonOperator(
        task_id='mark_end',
        python_callable=mark_end,
        provide_context=True,
        retries=5,
        retry_delay=timedelta(minutes=5)
    )
    create_metadata_schedule = PythonOperator(
        task_id='create_metadata_data_schedule',
        python_callable=insert_dag_metadata_schedule,
        provide_context=True,
        retries=5,
        retry_delay=timedelta(minutes=5)
    )
    end = EmptyOperator(task_id='end')

    start >> start_task >> get_token >> get_data_line >> end_task >> create_metadata_schedule >> end


dag = pipeline_hp_sianet_scheduler()
