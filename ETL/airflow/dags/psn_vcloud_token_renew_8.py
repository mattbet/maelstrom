#!/usr/bin/env python3
# psn_vcloud_token_renew_8.py
import logging
import time
import requests
import urllib3
from airflow.providers.postgres.hooks.postgres import PostgresHook

SERVICE_NAME = "psn_vcloud"
TOKEN_VALIDITY_MARGIN_SECONDS = 300
HTTP_TIMEOUT_SECONDS = 30
VERIFY_SSL = False
LOGGER = logging.getLogger(__name__)

if not VERIFY_SSL: 
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class TokenManagerError(Exception): 
    """Errore durante la gestione del token PSN."""

def get_access_token(postgres_conn_id: str, db_schema: str, serv_name: str = SERVICE_NAME) -> dict:
    """
    Funzione pura che riceve i parametri di connessione e schema dall'esterno.
    Ritorna un dizionario con l'access token e il vcloud url.
    """
    if not postgres_conn_id: 
        raise ValueError("Il parametro postgres_conn_id è vuoto")
    if not db_schema:
        raise ValueError("Il parametro db_schema è vuoto")
        
    LOGGER.info(
        "Recupero token per il servizio '%s' tramite connessione Airflow '%s' sullo schema '%s'", 
        serv_name, postgres_conn_id, db_schema
    )
    
    # Inizializza l'hook con l'ID passato dal DAG
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    
    select_query = f"""
        SELECT psn_org, fqdn, access_token, refresh_token, refresh_token_lifespan, updated_at_epoch 
        FROM {db_schema}.tab_psn_services 
        WHERE serv_name = %s;
    """
    
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(select_query, (serv_name,))
            row = cursor.fetchone()
            if row is None: 
                raise TokenManagerError(f"Nessun servizio trovato per serv_name='{serv_name}' nello schema '{db_schema}'")
            psn_org, fqdn, access_token, refresh_token, refresh_token_lifespan, updated_at_epoch = row

    if not psn_org or not fqdn or not refresh_token: 
        raise TokenManagerError(f"Dati incompleti nel DB per il servizio '{serv_name}'")
    
    refresh_token_lifespan = 0 if refresh_token_lifespan is None else refresh_token_lifespan
    updated_at_epoch = 0 if updated_at_epoch is None else updated_at_epoch
    
    current_epoch = int(time.time())
    refresh_expiration_epoch = int(updated_at_epoch) + int(refresh_token_lifespan)
    remaining_seconds = refresh_expiration_epoch - current_epoch

    clean_fqdn = fqdn.replace("https://", "").replace("http://", "").rstrip('/')
    base_vcloud_url = f"https://{clean_fqdn}"

    if remaining_seconds >= TOKEN_VALIDITY_MARGIN_SECONDS:
        LOGGER.info("Token valido per %s secondi. Nessun refresh necessario.", remaining_seconds)
        return {"access_token": access_token, "vcloud_url": base_vcloud_url}

    LOGGER.info("Rinnovo del token necessario per il servizio '%s'", serv_name)
    login_url = f"{base_vcloud_url}/oauth/tenant/{psn_org}/token"
    query_params = {"grant_type": "refresh_token", "refresh_token": refresh_token}
    headers = {"Accept": "application/json"}
    
    try:
        response = requests.post(login_url, params=query_params, headers=headers, verify=VERIFY_SSL, timeout=HTTP_TIMEOUT_SECONDS)
        response.raise_for_status()
        response_data = response.json()
        
        new_access_token = response_data.get("access_token")
        if not new_access_token: 
            raise TokenManagerError("La risposta non contiene un access_token valido")
            
        update_query = f"""
            UPDATE {db_schema}.tab_psn_services 
            SET access_token = %s, updated_at_epoch = %s 
            WHERE serv_name = %s;
        """
        hook.run(update_query, parameters=(new_access_token, int(time.time()), serv_name))
            
        LOGGER.info("Token rinnovato e salvato nel DB.")
        return {"access_token": new_access_token, "vcloud_url": base_vcloud_url}

    except Exception as e:
        raise TokenManagerError(f"Errore durante il refresh o l'aggiornamento DB: {str(e)}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("Script configurato come modulo. Eseguire tramite il DAG di Airflow.")
