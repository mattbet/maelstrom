# import_VisionOne_csv_dag.py
from datetime import datetime
import os
import csv
from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable


# Percorso assoluto della cartella all'interno del container Docker
CARTELLA_VOLUME = "/opt/airflow/data_in"
# Costante del Connection ID salvato nei pannelli di Airflow
CONN_ID = "postgres_airflow"
# Prende lo schema dalle variabili di Airflow (se manca usa 'maelstrom')
schema = Variable.get("db_schema", default_var="maelstrom")
# Imposta la tabella di destinazione
tabella_destinazione = "tab_vision_one"


default_params = {
    "nome_file_csv": Param(
        default="Trend_VisionONE Inventory.csv",
        type="string",
        description="Inserisci il nome del file CSV presente nella cartella data_in"
    )
}

# MAPPATURA UNIVOCA:
# Chiave = Nome esatto della colonna nell'header del tuo CSV (Case Sensitive)
# Valore = Nome esatto del campo corrispondente sul tuo Database Postgres
MAPPA_COLONNE = {
    "Endpoint name" : "endpoint_name",
    "OS name" : "os_name",
    "OS type" : "os_type",
    "OS version" : "os_version",
    "OS kernel" : "os_kernel",
    "Agent connection status" : "connected",
    "Endpoint GUID" : "endpoint_guid",
    "Last IP used" : "last_ip"
}

# Campi extra del CSV non inclusi nella mappa verranno scartati.

# Genera l'ordine corretto delle colonne DB per l'inserimento
COLONNE_DB_FINALI = list(MAPPA_COLONNE.values()) + ["import_date"]

with DAG(
    dag_id="import_Trend_Vision_One_csv",
    start_date=datetime(2026, 9, 10),
    schedule=None,
    catchup=False,
    params=default_params,
    tags=["postgres", "csv", "Trend_Vision_One"],
) as dag:

    @task
    def carica_e_inserisci_csv_dinamico(**kwargs):
        nome_file = kwargs["params"]["nome_file_csv"]
        percorso_completo = os.path.join(CARTELLA_VOLUME, nome_file)
        
        if not os.path.exists(percorso_completo):
            raise FileNotFoundError(f"Il file {percorso_completo} non esiste nel volume Docker ({percorso_completo}).")
            
        print(f"Lettura e filtraggio del file CSV: {percorso_completo}")
        
        timestamp_inserimento = datetime.now()
        righe_da_inserire = []
        
        with open(percorso_completo, mode='r', encoding='utf-8-sig') as f:
            # Rilevamento robusto del separatore
            prima_riga = f.readline()
            f.seek(0)
            
            # Se la virgola compare più del punto e virgola, usa la virgola, altrimenti il punto e virgola
            separatore = ',' if prima_riga.count(',') > prima_riga.count(';') else ';'
            print(f"Separatore rilevato automaticamente: '{separatore}'")
            
            lettore_csv = csv.DictReader(f, delimiter=separatore)
            
            # Rimuoviamo eventuali doppi apici residui dai nomi delle colonne letti dal CSV
            intestazioni_csv = [col.replace('"', '').strip() for col in (lettore_csv.fieldnames or [])]
            
            lettore_csv.fieldnames = intestazioni_csv
            print(f"Colonne rilevate nel CSV: {intestazioni_csv}")
            
            # Validazione: Controlla se le colonne necessarie esistono nel file
            colonne_mancanti = [col for col in MAPPA_COLONNE.keys() if col not in intestazioni_csv]
            if colonne_mancanti:
                raise ValueError(f"Errore critico: Le seguenti colonne attese non sono presenti nel CSV: {colonne_mancanti}")
            
            for riga_dizionario in lettore_csv:
                valori_riga = []
                
                for colonna_csv, colonna_db in MAPPA_COLONNE.items():
                    valore = riga_dizionario.get(colonna_csv)
                    
                    # Gestione stringhe vuote/NULL
                    if valore is not None:
                        valore = valore.strip().replace('"', '') # Rimuove eventuali virgolette interne ai dati
                        if valore == "":
                            valore = None
                            
                    # Normalizzazione per la colonna "Agent connection status" (campo DB "Connected")
                    if colonna_db.lower() == "connected" and valore is not None:
                        valore_pulito = valore.strip().lower()
                        if valore_pulito == "communicating":
                            valore = True
                        elif valore_pulito == "not communicating":
                            valore = False
                        else:
                            valore = None # Fallback se nel CSV compare un valore imprevisto
                              
                    valori_riga.append(valore)
                
                # Aggiunge il valore per la colonna 'import_date' alla fine
                valori_riga.append(timestamp_inserimento)
                righe_da_inserire.append(valori_riga)
        
        if not righe_da_inserire:
            print("Nessun dato utile da inserire nel Database.")
            return

        # Connessione al database tramite l'Hook di Airflow
        pg_hook = PostgresHook(postgres_conn_id=CONN_ID)
        
        print(f"Inserimento bulk di {len(righe_da_inserire)} righe nella tabella '{tabella_destinazione}'...")
        pg_hook.insert_rows(
            table=f"{schema}.{tabella_destinazione}",
            rows=righe_da_inserire,
            target_fields=COLONNE_DB_FINALI,
            commit_every=1000
        )
        
        print("Importazione completata con successo!")

    carica_e_inserisci_csv_dinamico()
