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
    event_filter = {
        "EntityIds": Variable.set("hp_mix_list_car"),
        "EventTypeIds": list_events,
        "MenuId": "string"
    }
    headers = {"Authorization": f"Bearer {get_token_bearer()}"}
    response = get(url_stream, headers=headers)
    if response.status_code == 200:
        return response.text
    else:
        raise Exception(f"Failed to get geodata: HTTP {response.status_code}")