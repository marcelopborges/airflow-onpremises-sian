import hashlib
import json
import logging
from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from requests import get, post
from io import BytesIO
import pandas as pd
from airflow.providers.google.cloud.hooks.gcs import GCSHook

last_call_timestamp = None
url_api_sianet = Variable.get("hp_sianet_url_api")
auth_token_basic_sianet = Variable.get("hp_sianet_auth_token_basic")
hp_sianet_url_trajeto_ocioso = Variable.get("hp_sianet_url_trajeto_ocioso")
hp_gcp_bucket_name_raw = Variable.get("hp_gcp_bucket_name_raw")

token_data = {
    "token": None,
    "timestamp": None
}

def generate_hash(*args):
    hash_input = "".join(args)
    return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()

def get_token_sianet():
    global token_data
    if token_data["token"] is not None and (datetime.now() - token_data["timestamp"]) < timedelta(seconds=1800):
        return token_data["token"]
    else:
        url = f"{url_api_sianet}"
        payload = {}
        headers = {
            'Authorization': f'Basic {auth_token_basic_sianet}'
        }
        response = post(url, headers=headers, data=payload)
        if response.status_code == 200:
            data = response.json()
            token_data["token"] = data['token']
            token_data["timestamp"] = datetime.now()
            return data['token']
        else:
            raise Exception(f"Failed to get token: HTTP {response.status_code}")

def get_data_line_sianet(**kwargs):
    url = f"{hp_sianet_url_trajeto_ocioso}"
    headers = {"Authorization": f"Bearer {get_token_sianet()}"}
    response = get(url, headers=headers)
    if response.status_code == 200:
        return response.text
    else:
        raise Exception(f"Failed to get geodata: HTTP {response.status_code}")

def insert_line_data_schedule(**kwargs):
    ti = kwargs['ti']
    linha_data_json = ti.xcom_pull(task_ids='get_data_line')
    linha_data = json.loads(linha_data_json)
    rows_to_insert = []

    try:
        for k, v in linha_data['dados'].items():
            linha = v['linha']['numLinha']
            descricao = v['linha']['descricao']
            for direcao in ['ida', 'volta']:
                pontos_trajeto = v['trajeto'].get(direcao, {}).get('PONTOS_TRAJETO', [])
                if pontos_trajeto:
                    row = {
                        "num_linha": linha,
                        "descricao": descricao,
                        "direcao": direcao,
                        "pontos_geolocalizacao": json.dumps(pontos_trajeto)
                    }
                    rows_to_insert.append(row)
    except Exception as e:
        logging.error(f"Ocorreu um erro: {e}")

    df = pd.DataFrame(rows_to_insert)

    # Salva o DataFrame em formato Parquet no buffer
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    # Envia o arquivo Parquet diretamente para o bucket GCP usando GCSHook
    gcs_hook = GCSHook(gcp_conn_id='gcp')
    logical_date = kwargs['logical_date']
    gcs_hook.upload(
        bucket_name=hp_gcp_bucket_name_raw,
        object_name=f"sianet/dados_linhas/dados_linhas_{logical_date.strftime('%Y-%m-%d')}.parquet",
        data=buffer.getvalue(),
        mime_type='application/octet-stream'
    )

    logging.info(f"Arquivo Parquet enviado para o bucket GCP: sianet/dados_linhas/dados_linhas_{logical_date.strftime('%Y-%m-%d')}.parquet")

def insert_dag_metadata_dados_linha(**kwargs):
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

    logging.info(f"Arquivo JSON de metadados enviado para o bucket GCP: sianet/metadata/pipeline_hp_sianet_{kwargs['ds']}.json")

def mark_start(**context):
    start = datetime.now()
    context['ti'].xcom_push(key='start_time', value=start)
    print(f"Mark start at {start}")

def mark_end(**context):
    end = datetime.now()
    context['ti'].xcom_push(key='end_time', value=end)
    print(f"Mark end at {end}")


@dag(start_date=datetime(2024, 4, 5),
     schedule='30 11 * * *',
     catchup=False,
     tags=['airbyte', 'HP', 'Sianet', 'Escala'])
def pipeline_hp_sianet():
    start = EmptyOperator(task_id='start')
    start_task = PythonOperator(
        task_id='mark_start',
        python_callable=mark_start,
        provide_context=True,
    )
    get_token = PythonOperator(
        task_id='get_token',
        python_callable=get_token_sianet,
        provide_context=True,
        retries=5,
        retry_delay=timedelta(minutes=5),
    )
    get_data_line = PythonOperator(
        task_id='get_data_line',
        python_callable=get_data_line_sianet,
        provide_context=True,
        retries=5,
        retry_delay=timedelta(minutes=5),
    )

    insert_line = PythonOperator(
        task_id='insert_line_data',
        python_callable=insert_line_data_schedule,
        provide_context=True,
        retries=5,
        retry_delay=timedelta(minutes=5),
    )

    create_metadata_data_line = PythonOperator(
        task_id='create_metadata_data_line',
        python_callable=insert_dag_metadata_dados_linha,
        provide_context=True,
        retries=5,
        retry_delay=timedelta(minutes=5)
    )
    end_task = PythonOperator(
        task_id='mark_end',
        python_callable=mark_end,
        provide_context=True,
    )
    end = EmptyOperator(task_id='end')

    start >> start_task >> get_token >> get_data_line >> insert_line >> end_task >> create_metadata_data_line >> end

dag = pipeline_hp_sianet()
