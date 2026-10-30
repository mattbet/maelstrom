# import_zabbix_json_dag
from datetime import datetime
import json
import logging
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.param import Param
from airflow.models import Variable

# Percorso assoluto della cartella all'interno del container Docker
CARTELLA_VOLUME = "/opt/airflow/data_in"
# Costante del Connection ID salvato nei pannelli di Airflow
CONN_ID = "postgres_airflow"
# Prende lo schema dalle variabili di Airflow (se manca usa 'maelstrom')
schema = Variable.get("db_schema", default_var="maelstrom")
# Imposta la tabella di destinazione
tabella_destinazione = "tab_zabbix_hosts"

with DAG(
    dag_id='import_Zabbix_json',
    start_date=datetime(2026, 9, 10),
    schedule_interval=None,  # Esecuzione solo manuale/trigger
    catchup=False,
    tags=['zabbix', 'json', 'postgres'],
    # Configura il campo di input che apparirà nell'interfaccia di Airflow
    params={
        # Chiediamo all'utente solo il nome del file inserito nel volume
        "nome_file_json": Param(
            default="Zabbix_hosts.json",
            type="string",
            description="Inserisci il nome del file JSON presente nella cartella condivisa data_in (es. dati.json)."
        )
    }
) as dag:

    def importa_zabbix_json(**kwargs):
        # 1. Recupera il nome del file inserito dall'utente tramite i params
        nome_file = kwargs['params']['nome_file_json']
        logging.info(f"Inizio elaborazione del file: {nome_file}")
        
        # Costruisce il percorso completo unendo la cartella del volume al nome del file
        percorso_completo = os.path.join(CARTELLA_VOLUME, nome_file)
        logging.info(f"Tentativo di lettura del file nel volume: {percorso_completo}")
        
        # 1. Verifica l'esistenza del file e lo legge
        try:
            with open(percorso_completo, 'r', encoding='utf-8') as f:
                dati = json.load(f)
                logging.info(f"Tipo di dato principale: {type(dati)}. Chiavi presenti: {list(dati.keys()) if isinstance(dati, dict) else 'È una lista'}")
        except FileNotFoundError:
            raise FileNotFoundError(
                f"Errore: Il file '{nome_file}' non è stato trovato nella cartella del volume '{CARTELLA_VOLUME}'. "
                f"Verifica che il file sia presente nella cartella di Windows legata a 'data_in'."
            )
        except json.JSONDecodeError as e:
            raise ValueError(f"Il file {nome_file} non contiene un JSON valido: {e}")
        
        # 2. Isola il gruppo "hosts"
        if "zabbix_export" not in dati or "hosts" not in dati["zabbix_export"]:
            raise KeyError("La struttura JSON non è valida: manca la chiave 'zabbix_export' o 'hosts' al suo interno.")
        
        lista_hosts = dati["zabbix_export"]["hosts"]
        if not lista_hosts:
            logging.warning("Il gruppo 'hosts' è vuoto. Processo terminato senza inserimenti.")
            return


        # Normalizza in lista se l'elemento fosse un dizionario singolo
        if isinstance(lista_hosts, dict):
            lista_hosts = [lista_hosts]
        logging.info(f"Trovati {len(lista_hosts)} elementi da caricare.")

        # 3. Filtro e trasformazione dei dati richiesti
        # Definiamo le colonne fisse che manderemo a PostgreSQL
        colonne = ["host", "name", "ip"]
        valori_da_inserire = []
        
        for host in lista_hosts:
            # Estrazione dei campi principali di primo livello
            host_field = host.get("host")
            name_field = host.get("name")
            ip_field = None
            
            # Estrazione sicura del campo 'ip' da 'interfaces'
            interfaces = host.get("interfaces", [])
            
            # Gestione se interfaces è una lista (standard Zabbix) o un singolo dizionario
            if isinstance(interfaces, list) and len(interfaces) > 0:
                ip_field = interfaces[0].get("ip")
            elif isinstance(interfaces, dict):
                ip_field = interfaces.get("ip")
                
            # Aggiunge alla lista dei record solo se i campi minimi sono presenti (opzionale)
            valori_da_inserire.append((host_field, name_field, ip_field))

        # 4. Connessione a PostgreSQL e inserimento dati
        pg_hook = PostgresHook(postgres_conn_id=CONN_ID)
        
        pg_hook.insert_rows(
            table=f"{schema}.{tabella_destinazione}",
            rows=valori_da_inserire,
            target_fields=colonne
        )
        logging.info(f"Caricamento di {len(valori_da_inserire)} righe completato con successo su '{tabella_destinazione}'.")

    task_carica_dati = PythonOperator(
        task_id='elabora_e_carica_host_zabbix',
        python_callable=importa_zabbix_json,
    )
