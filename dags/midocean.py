"""
Airflow DAG to convert all .xlsx files in the data directory to NDJSON and upload to GCS.
"""
import os
import json
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta, timezone
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from google.cloud import storage
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.auth import default
import tempfile


SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
SERVICE_ACCOUNT_FILE = './key-json.json'

BUCKET_NAME = "matthieu_proto_bucket"
FOLDER_ID = '1Mk0jjPdj4BA_40yoRJbTl7_q2z9R4R6_'

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(DATA_DIR)
DATA_PATH = os.path.join(PARENT_DIR, "data")

def get_drive_service(conn_id="gcp_connection"):
    hook = GoogleBaseHook(gcp_conn_id=conn_id, delegate_to=None)
    creds = hook.get_credentials()
    return build('drive', 'v3', credentials = creds)

def list_excel_files(service, folder_id, path_prefix=""):
    query = f"'{folder_id}' in parents and trashed = false"
    results = service.files().list(q=query, fields="files(id, name, mimeType)").execute()
    files = results.get('files', [])
    xlsx_files = []

    for f in files:
        if f['mimeType'] == 'application/vnd.google-apps.folder':
            new_prefix = os.path.join(path_prefix, f['name'])
            xlsx_files += list_excel_files(service, f['id'], new_prefix)
        elif f['mimeType'] == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
            file_path = os.path.join(path_prefix, f['name'])
            f['path'] = file_path
            xlsx_files.append(f)
    return xlsx_files

def upload_to_gcs(local_path, gcs_path, bucket_name, conn_id="gcp_connection"):
    hook = GoogleBaseHook(gcp_conn_id=conn_id)
    creds = hook.get_credentials()
    # client = storage.Client(credentials=creds, project=creds.project_id)
    client = storage.Client(credentials=creds, project='matthieu_proto_bucket')
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.chunk_size = 5 * 1024 * 1024 
    blob.upload_from_filename(local_path, timeout=600)
    print(f"Uploaded to gs://{bucket_name}/{gcs_path}")

def process_drive_files(**kwargs):
    service = get_drive_service()
    files = list_excel_files(service, FOLDER_ID)
    today = datetime.now(timezone.utc).date().isoformat()

    for file in files:
        request = service.files().get_media(fileId=file['id'])
        fh = BytesIO()
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while not done:
            _, done = downloader.next_chunk()

        fh.seek(0)
        df = pd.read_excel(fh, dtype=str)
        now = datetime.now(timezone.utc).isoformat()

        with tempfile.NamedTemporaryFile(mode='w+', suffix='.json', delete=False) as tmp:
            for _, row in df.iterrows():
                record = {
                    "loaded_at": now,
                    "data": row.dropna().to_dict()
                }
                tmp.write(json.dumps(record) + "\n")
            tmp.flush()

            ndjson_path = excel_to_ndjson(fh)
            base_path = os.path.splitext(file['path'])[0]
            gcs_path = f"{base_path}/loaded_on={today}/{file['name']}.json"
            upload_to_gcs(ndjson_path, gcs_path, BUCKET_NAME)
            os.remove(ndjson_path)

def excel_to_ndjson(file_stream: BytesIO) -> str:
    """Converts an Excel file (from BytesIO) to an NDJSON file on disk and returns its path."""
    df = pd.read_excel(file_stream, dtype=str)
    now = datetime.now(timezone.utc).isoformat()

    # Create a temp file to store the NDJSON
    tmp = tempfile.NamedTemporaryFile(mode='w+', suffix='.json', delete=False)
    for _, row in df.iterrows():
        record = {
            "loaded_at": now,
            "data": row.dropna().to_dict()
        }
        tmp.write(json.dumps(record) + "\n")
    tmp.flush()
    return tmp.name

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(days=1),
}

dag = DAG(
    'drive_xlsx_to_gcs',
    default_args=default_args,
    description='Convert xlsx to ndjson and upload to GCS',
    catchup=False,
)

process_task = PythonOperator(
    task_id='process_google_drive_files',
    python_callable=process_drive_files,
    dag=dag,
)
