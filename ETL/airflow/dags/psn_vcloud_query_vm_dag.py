# dags/psn_vcloud_query_vm_dag_1.py
from datetime import datetime, timedelta
import json
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from psn_vcloud_token_renew_8 import get_access_token

default_args = {
    'owner': 'data_team',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Costante del Connection ID salvato nei pannelli di Airflow
CONN_ID = "postgres_airflow"
# Prende lo schema dalle variabili di Airflow (se manca usa 'maelstrom')
schema = Variable.get("db_schema", default_var="maelstrom")

def clean_json_field(field_value):
    if isinstance(field_value, (dict, list)):
        return json.dumps(field_value)
    return field_value

def esegui_rinnovo_token(**kwargs):
    """Task 1: Prende lo schema dalle variabili e chiama lo script passandogli i dati"""

    
    # Chiama la funzione dello script esterno passando i parametri richiesti
    risultato = get_access_token(postgres_conn_id=CONN_ID, db_schema=schema)
    # Ritorna il dizionario (Airflow lo salverà automaticamente in XCom)
    return risultato

def estrai_e_carica_vms(**kwargs):
    """Task 2: Estrae i dati vCloud e aggiorna la tabella usando la connessione di Airflow"""
    ti = kwargs['ti']
    
    # Recupera i dati generati dal Task 1 tramite XCom
    vcloud_data = ti.xcom_pull(task_ids='refresh_vcloud_token')
    if not vcloud_data:
        raise ValueError("Dati del token non trovati in XCom.")
        
    token = vcloud_data.get('access_token')
    vcloud_url = vcloud_data.get('vcloud_url')

    auth_headers = {
        "Accept": "application/*+json;version=38.0",
        "Authorization": f"Bearer {token}" 
    }

    query_url = f"{vcloud_url}/api/query?type=vm"
    print(f"📡 Estrazione dati VM da: {query_url}")
    response = requests.get(query_url, headers=auth_headers, verify=False, timeout=30)
    
    if response.status_code != 200:
        raise Exception(f"Errore API VM: {response.status_code} - {response.text}")
        
    records = response.json().get('record', [])
    if not records:
        print("Nessuna VM trovata.")
        return

    marca_temporale = str(datetime.now()).split('.')[0]
    dati_da_inserire = [
         (
             item.get('href').split('/')[-1] if item.get('href') else None, 
             item.get('_type'), 
             clean_json_field(item.get('link')),          
             clean_json_field(item.get('metadata')),      
             item.get('href'), 
             item.get('id'), 
             item.get('type'), 
             clean_json_field(item.get('otherAttributes')),
             item.get('name'), 
             item.get('containerName'), 
             item.get('container'), 
             item.get('ownerName'), 
             item.get('owner'), 
             item.get('vdcName'), 
             item.get('vdc'), 
             item.get('description'), 
             item.get('vappScopedLocalId'), 
             item.get('isVAppTemplate'), 
             item.get('isDeleted'), 
             item.get('guestOs'), 
             item.get('detectedGuestOs'), 
             item.get('numberOfCpus'), 
             item.get('memoryMB'), 
             item.get('status'), 
             item.get('networkName'), 
             item.get('network'), 
             item.get('ipAddress'), 
             item.get('isBusy'), 
             item.get('isDeployed'), 
             item.get('isPublished'), 
             item.get('catalogName'), 
             item.get('hardwareVersion'), 
             item.get('vmToolsStatus'), 
             item.get('isInMaintenanceMode'), 
             item.get('isAutoNature'), 
             item.get('storageProfileName'), 
             clean_json_field(item.get('snapshot')),      
             item.get('snapshotCreated'), 
             item.get('gcStatus'), 
             item.get('autoUndeployDate'), 
             item.get('autoDeleteDate'), 
             item.get('isAutoUndeployNotified'), 
             item.get('isAutoDeleteNotified'), 
             item.get('isComputePolicyCompliant'), 
             item.get('vmSizingPolicyId'), 
             item.get('vmPlacementPolicyId'), 
             item.get('encrypted'), 
             item.get('dateCreated'), 
             item.get('totalStorageAllocatedMb'), 
             item.get('isExpired'), 
             item.get('defaultStoragePolicyName'), 
             item.get('hasVgpuPolicy'), 
             item.get('firmware'), 
             item.get('tpmPresent'), 
             item.get('replicationState'), 
             marca_temporale
         )
        for item in records
    ]

    # Utilizza la costante del DB impostata nel DAG per caricare i dati finali
    pg_hook = PostgresHook(postgres_conn_id=CONN_ID)
    pg_hook.insert_rows(
        table=f'{schema}.tab_psn_vcloud_vm', 
        rows=dati_da_inserire, 
        target_fields=[
            'vcloud_extracted_id', '_type', 'link', 'metadata', 'href', 'id', 'type', 
            'other_attributes', 'name', 'container_name', 'container', 'owner_name', 
            'owner', 'vdc_name', 'vdc', 'description', 'vapp_scoped_local_id', 
            'is_vapp_template', 'is_deleted', 'guest_os', 'detected_guest_os', 
            'number_of_cpus', 'memory_mb', 'status', 'network_name', 'network', 
            'ip_address', 'is_busy', 'is_deployed', 'is_published', 'catalog_name', 
            'hardware_version', 'vm_tools_status', 'is_in_maintenance_mode', 
            'is_auto_nature', 'storage_profile_name', 'snapshot', 'snapshot_created', 
            'gc_status', 'auto_undeploy_date', 'auto_delete_date', 'is_auto_undeploy_notified', 
            'is_auto_delete_notified', 'is_compute_policy_compliant', 'vm_sizing_policy_id', 
            'vm_placement_policy_id', 'encrypted', 'date_created', 'total_storage_allocated_mb', 
            'is_expired', 'default_storage_policy_name', 'has_vgpu_policy', 'firmware', 
            'tpm_present', 'replication_state', 'marca_temporale'
        ]
    )
    print("✅ Tabella maelstrom.tab_psn_vcloud_vm aggiornata su Postgres.")

with DAG(
    dag_id='psn_vcloud_estrazione_vm',
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_runs=1
) as dag:

    task_refresh_token = PythonOperator(
        task_id='refresh_vcloud_token',
        python_callable=esegui_rinnovo_token
    )

    task_vms = PythonOperator(
        task_id='estrai_vms_to_postgres',
        python_callable=estrai_e_carica_vms
    )

    task_refresh_token >> task_vms
