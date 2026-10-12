from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.models import Variable
import json
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def process_and_save_tokens(response):
    """
    Legge la risposta JSON del PSN, estrae il nuovo Access Token 
    e il nuovo Refresh Token, aggiornando le variabili Airflow.
    """
    token_data = response.json()
    new_access_token = token_data.get('access_token')
    new_refresh_token = token_data.get('refresh_token')
    
    if new_access_token:
        Variable.set("psn_baas_token", new_access_token)
        logging.info("Nuovo Access Token (Bearer) memorizzato con successo.")
        
    if new_refresh_token:
        Variable.set("psn_refresh_token", new_refresh_token)
        logging.info("Nuovo Refresh Token ruotato e memorizzato per il prossimo ciclo.")
    else:
        logging.warning("L'API non ha restituito un nuovo refresh token. Verrà riutilizzato quello attuale.")
        
    return new_access_token

with DAG(
    dag_id='psn_baas_auth_refresh',
    default_args=default_args,
    description='Rinnova il Bearer Token PSN utilizzando solo il Refresh Token memorizzato',
    schedule_interval='0 */2 * * *',
    catchup=False
) as dag:

    refresh_token_task = HttpOperator(
        task_id='refresh_token_task',
        http_conn_id='psn_baas_api',
        endpoint='auth/oauth/token',
        method='POST',
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        # Rimosse le voci client_id e client_secret
        data={
            "grant_type": "refresh_token",
            "refresh_token": "{{ var.value.psn_refresh_token }}"
        },
        response_filter=process_and_save_tokens,
    )
