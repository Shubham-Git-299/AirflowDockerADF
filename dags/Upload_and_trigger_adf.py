import os
import requests
from azure.storage.blob import BlobServiceClient
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime

CONN_STR = Variable.get("AZURE_BLOB_CONN_STR")  
CONTAINER_NAME = "raw"
LOCAL_DATA_FOLDER = "/opt/airflow/dags/Data"  # Adjust path inside container

FILES_TO_UPLOAD = ["orders.csv", "people.csv", "returns.csv"]

TENANT_ID = Variable.get("AZURE_TENANT_ID")
CLIENT_ID = Variable.get("AZURE_CLIENT_ID")
CLIENT_SECRET = Variable.get("AZURE_CLIENT_SECRET")
SUBSCRIPTION_ID = Variable.get("AZURE_SUBSCRIPTION_ID")
RESOURCE_GROUP = Variable.get("AZURE_RESOURCE_GROUP")
DATA_FACTORY_NAME = Variable.get("AZURE_DATA_FACTORY_NAME")
PIPELINE_NAME = "Pipeline1"


def upload_files_to_blob():
    """Uploads all CSV files to Azure Blob Storage"""
    client = BlobServiceClient.from_connection_string(CONN_STR)

    for file_name in FILES_TO_UPLOAD:
        local_path = os.path.join(LOCAL_DATA_FOLDER, file_name)
        blob_client = client.get_blob_client(container=CONTAINER_NAME, blob=f"Data/{file_name}")

        with open(local_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
            print(f"✅ Uploaded {file_name} to Azure Blob Storage")

    print("All files uploaded successfully!")

def trigger_adf_pipeline(**kwargs):
    """Triggers Azure Data Factory pipeline via REST API"""
    # 1️⃣ Get access token
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token"
    payload = {
        'grant_type': 'client_credentials',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'resource': 'https://management.azure.com/'
    }
    token = requests.post(url, data=payload).json().get('access_token')

    # 2️⃣ Trigger pipeline
    trigger_url = (
        f"https://management.azure.com/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP}"
        f"/providers/Microsoft.DataFactory/factories/{DATA_FACTORY_NAME}/pipelines/{PIPELINE_NAME}/createRun"
        f"?api-version=2018-06-01"
    )

    headers = {'Authorization': f'Bearer {token}'}
    response = requests.post(trigger_url, headers=headers)

    if response.status_code == 200:
        run_id = response.json().get('runId')
        print(f"✅ Pipeline triggered successfully. RunId: {run_id}")
    else:
        raise Exception(f"Pipeline trigger failed: {response.text}")

with DAG(
    dag_id='upload_and_trigger_adf',
    start_date=datetime(2025, 8, 3),
    schedule_interval=None,
    catchup=False
) as dag:

    upload_task = PythonOperator(
        task_id='upload_csvs',
        python_callable=upload_files_to_blob
    )

    trigger_adf_task = PythonOperator(
        task_id='trigger_adf_pipeline',
        python_callable=trigger_adf_pipeline
    )

    upload_task >> trigger_adf_task
