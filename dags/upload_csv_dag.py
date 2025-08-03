from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import os

# Azure Storage Configuration
CONN_STR = "DefaultEndpointsProtocol=https;AccountName=airflowdockerstorage;AccountKey=Z3vW2IXp3NlKRx3VBeC9TwlCna5En9MYtAZOf2SAVfaQBfm4sgHwT4n24gz2jCv83u7u1OCp3YQq+AStS3WzNw==;EndpointSuffix=core.windows.net"
CONTAINER = "raw"

# Local folder path (inside Docker container)
LOCAL_DATA_FOLDER = "/opt/airflow/dags/Data"

# Files to upload
FILES_TO_UPLOAD = [
    "orders.csv",
    "people.csv",
    "returns.csv"
]

def upload_files():
    client = BlobServiceClient.from_connection_string(CONN_STR)
    for file_name in FILES_TO_UPLOAD:
        local_path = os.path.join(LOCAL_DATA_FOLDER, file_name)
        blob_client = client.get_blob_client(container=CONTAINER, blob=f"Data/{file_name}")
        with open(local_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        print(f"Uploaded {file_name} to Azure Blob Storage in Data folder")

def list_blobs():
    client = BlobServiceClient.from_connection_string(CONN_STR)
    container_client = client.get_container_client(CONTAINER)
    print("📂 Blobs in container:")
    for blob in container_client.list_blobs():
        print("Blob:", blob.name)
    print("Done listing blobs.")

with DAG(
    dag_id="upload_multiple_csvs",
    start_date=datetime(2025, 8, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    upload_task = PythonOperator(
        task_id="upload_multiple_files",
        python_callable=upload_files
    )

    list_task = PythonOperator(
        task_id="list_blobs_after_upload",
        python_callable=list_blobs
    )

    upload_task >> list_task
