from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from azure.storage.blob import BlobServiceClient

# ---------------------------
# Azure Storage Configuration
# ---------------------------
CONN_STR = "DefaultEndpointsProtocol=https;AccountName=airflowdockerstorage;AccountKey=Z3vW2IXp3NlKRx3VBeC9TwlCna5En9MYtAZOf2SAVfaQBfm4sgHwT4n24gz2jCv83u7u1OCp3YQq+AStS3WzNw==;EndpointSuffix=core.windows.net"
CONTAINER = "raw"
LOCAL_FILE = "/opt/airflow/dags/sample_test.csv"  # Path inside container

def list_blobs():
    client = BlobServiceClient.from_connection_string(CONN_STR)
    container_client = client.get_container_client(CONTAINER)
    print("Listing blobs in container:")
    for blob in container_client.list_blobs():
        print("Blob:", blob.name)
    print("✅ Done listing blobs.")

def upload_blob():
    client = BlobServiceClient.from_connection_string(CONN_STR)
    blob_client = client.get_blob_client(container=CONTAINER, blob="sample_test.csv")
    with open(LOCAL_FILE, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    print("✅ File uploaded successfully!")

with DAG(
    dag_id="upload_and_list_blobs",
    start_date=datetime(2025, 8, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    list_before = PythonOperator(
        task_id="list_blobs_before",
        python_callable=list_blobs
    )

    upload_task = PythonOperator(
        task_id="upload_blob_file",
        python_callable=upload_blob
    )

    list_after = PythonOperator(
        task_id="list_blobs_after",
        python_callable=list_blobs
    )

    list_before >> upload_task >> list_after
