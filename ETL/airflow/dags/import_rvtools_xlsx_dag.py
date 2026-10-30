from datetime import datetime
import os
import pandas as pd
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
tabella_destinazione = "tab_rvtools_vm"

# Definizione dei parametri del DAG con valore di default richiesto
default_params = {
    "nome_file_xlsx": Param(
        default="OnPrem.xlsx",
        type="string",
        description="Inserisci il nome del file Excel (es. OnPrem.xlsx) presente nella cartella data_in"
    )
}

# Mappatura delle colonne dirette (per quelle semplici)
# Chiave: Nome esatto della colonna nel foglio vInfo di RVTools
# Valore: Nome del campo sul Database Postgres
MAPPA_COLONNE_DIRETTE = {
    "VM": "vm_name",
    "DNS Name": "dns_name",
    "CPUs": "cpu",
    "Memory": "ram_mb",
    "Total disk capacity MiB": "disk_mb",
    "Primary IP Address": "ip",
    "VM UUID": "uuid"
}

# Elenco finale dei campi che verranno inseriti nel Database nell'ordine corretto
COLONNE_DB_FINALI = [
    "vm_name",
    "powerstate",
    "dns_name",
    "cpu",
    "ram_mb",
    "disk_mb",
    "ip",
    "uuid",
    "os",
    "import_date"
]

with DAG(
    dag_id="import_rvtools_xlsx",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params=default_params,
    tags=["postgres", "excel", "rvtools"],
) as dag:

    @task
    def carica_e_inserisci_xlsx(**kwargs):
        nome_file = kwargs["params"]["nome_file_xlsx"]
        
        # Se l'utente scrive solo "OnPrem", aggiungiamo l'estensione .xlsx per comodità
        if not nome_file.lower().endswith('.xlsx'):
            nome_file += '.xlsx'
            
        percorso_completo = os.path.join(CARTELLA_VOLUME, nome_file)
        
        if not os.path.exists(percorso_completo):
            raise FileNotFoundError(f"Il file {percorso_completo} non esiste nella cartella data_in.")
            
        print(f"Lettura del file Excel: {percorso_completo} - Foglio: vInfo")
        
        # Lettura del foglio specifico tramite pandas (richiede openpyxl installato)
        df = pd.read_excel(percorso_completo, sheet_name="vInfo")
        
        # Pulizia preliminare dei nomi delle colonne (rimozione spazi vuoti)
        df.columns = [str(col).strip() for col in df.columns]
        
        # Validazione colonne minime necessarie
        colonne_richieste = list(MAPPA_COLONNE_DIRETTE.keys()) + [
            "Powerstate", 
            "OS according to the VMware Tools", 
            "OS according to the configuration file"
        ]
        colonne_mancanti = [col for col in colonne_richieste if col not in df.columns]
        if colonne_mancanti:
            raise ValueError(f"Colonne mancanti nel foglio vInfo: {colonne_mancanti}")
            
        timestamp_inserimento = datetime.now()
        righe_da_inserire = []
        
        # Iterazione sulle righe del DataFrame
        for _, riga in df.iterrows():
            valori_riga = {}
            
            # 1. Mappatura colonne dirette
            for col_excel, col_db in MAPPA_COLONNE_DIRETTE.items():
                valore = riga[col_excel]
                # Gestione dei valori nulli di Pandas (NaN)
                if pd.isna(valore):
                    valore = None
                elif isinstance(valore, str):
                    valore = valore.strip()
                valori_riga[col_db] = valore
            
            # 2. Conversione Powerstate in booleano (poweredOn -> True, poweredOff -> False)
            p_state = str(riga["Powerstate"]).strip()
            if p_state == "poweredOn":
                valori_riga["powerstate"] = True
            elif p_state == "poweredOff":
                valori_riga["powerstate"] = False
            else:
                valori_riga["powerstate"] = None
                
            # 3. Logica condizionale per la colonna OS
            os_tools = riga["OS according to the VMware Tools"]
            os_config = riga["OS according to the configuration file"]
            
            # Se os_tools non è nullo/vuoto usa quello, altrimenti usa os_config
            if pd.notna(os_tools) and str(os_tools).strip() != "":
                valori_riga["os"] = str(os_tools).strip()
            elif pd.notna(os_config) and str(os_config).strip() != "":
                valori_riga["os"] = str(os_config).strip()
            else:
                valori_riga["os"] = None
                
            # 4. Aggiunta della data/ora corrente
            valori_riga["import_date"] = timestamp_inserimento
            
            # Costruisce la lista ordinata secondo l'ordine di COLONNE_DB_FINALI
            riga_ordinata = [valori_riga[campo] for campo in COLONNE_DB_FINALI]
            righe_da_inserire.append(riga_ordinata)
            
        if not righe_da_inserire:
            print("Nessun dato trovato nel file Excel.")
            return
            
        # Connessione e inserimento in Postgres
        pg_hook = PostgresHook(postgres_conn_id=CONN_ID)
        
        print(f"Inserimento di {len(righe_da_inserire)} righe nella tabella '{tabella_destinazione}'...")
        pg_hook.insert_rows(
            table=f"{schema}.{tabella_destinazione}",
            rows=righe_da_inserire,
            target_fields=COLONNE_DB_FINALI,
            commit_every=1000
        )
        print("Importazione completata con successo!")

    carica_e_inserisci_xlsx()
