# import_ad_csv_dag.py
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
tabella_destinazione = "ad_export"


default_params = {
    "nome_file_csv": Param(
        default="AD_Servers_Export.csv",
        type="string",
        description="Inserisci il nome del file CSV presente nella cartella data_in"
    )
}

# MAPPATURA UNIVOCA:
# Chiave = Nome esatto della colonna nell'header del tuo CSV (Case Sensitive)
# Valore = Nome esatto del campo corrispondente sul tuo Database Postgres
MAPPA_COLONNE = {
    "Name": "name",
    "dNSHostName": "dns_hostname",
    "OperatingSystem": "operating_system",
    "OperatingSystemVersion": "operating_system_version",
    "IPv4Address": "ipv4_address",
    "Enabled": "enabled",
    "Description": "description",
    "DistinguishedName": "distinguished_name"
}

# Campi extra del CSV (LastLogonDate, PasswordLastSet) non inclusi nella mappa verranno scartati.

# Genera l'ordine corretto delle colonne DB per l'inserimento
COLONNE_DB_FINALI = list(MAPPA_COLONNE.values()) + ["import_date"]

with DAG(
    dag_id="import_active_directory_csv",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params=default_params,
    tags=["postgres", "csv", "active-directory"],
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
            # Rileva automaticamente se il CSV usa la virgola, il punto e virgola o la tabulazione come separatore
            try:
                contenuto_iniziale = f.read(4096)
                dialetto = csv.Sniffer().sniff(contenuto_iniziale, delimiters=[',', ';', '\t'])
                f.seek(0)
                lettore_csv = csv.DictReader(f, delimiter=';') # Fallback sul punto e virgola tipico di Windows
            except csv.Error:
                # Fallback standard se lo sniffer fallisce (assume virgola)
                f.seek(0)
                lettore_csv = csv.DictReader(f)
            
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
                    
                    # Normalizzazione specifica per il tipo BOOLEAN di Postgres (campo Enabled)
                    if colonna_db == "enabled" and valore is not None:
                        valore_pulito = valore.lower()
                        if valore_pulito in ['true', '1', 't', 'yes', 'y']:
                            valore = True
                        elif valore_pulito in ['false', '0', 'f', 'no', 'n']:
                            valore = False
                        else:
                            valore = None
                            
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
