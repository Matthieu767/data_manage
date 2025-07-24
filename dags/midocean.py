"""
Airflow DAG to convert all .xlsx files in the data directory to NDJSON and upload to GCS.
"""
import os
import glob
import pandas as pd
import json
from datetime import datetime, timezone
from google.cloud import storage
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime


def excel_to_ndjson(file_path, output_path):
    df = pd.read_excel(file_path, dtype=str)
    now = datetime.now(timezone.utc).isoformat()
    with open(output_path, "w", encoding="utf-8") as f:
        for _, row in df.iterrows():
            row_dict = row.dropna().to_dict()
            record = {
                "loaded_at": now,
                "data": row_dict
            }
            f.write(json.dumps(record) + "\n")


def upload_to_gcs(local_path, gcs_path, bucket_name):
    client = storage.Client(project='datamanagementplatformdev')
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)
    print(f"Uploaded {local_path} to gs://{bucket_name}/{gcs_path}")

BUCKET_NAME = "datamanagementplatformdev_mid_ocean_land"
DATA_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(DATA_DIR)
DATA_PATH = os.path.join(PARENT_DIR, "data")

def process_xlsx_files(keep_local_json=False):
    xlsx_files = glob.glob(os.path.join(DATA_PATH, "midocean/*.xlsx"))
    today = datetime.now(timezone.utc).date().isoformat()
    for file_path in xlsx_files:
        table_name = os.path.splitext(os.path.basename(file_path))[0]
        json_filename = f"{table_name}.json"
        json_path = os.path.join(DATA_PATH, json_filename)
        excel_to_ndjson(file_path, json_path)
        gcs_path = f"{table_name}/loaded_on={today}/{os.path.basename(json_filename)}"
        upload_to_gcs(json_path, gcs_path, BUCKET_NAME)
        if not keep_local_json:
            os.remove(json_path)
            print(f"Deleted local file {json_path}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(days=1),
}

dag = DAG(
    'midocean',
    default_args=default_args,
    description='Convert xlsx to ndjson and upload to GCS',
    catchup=False,
)

process_task = PythonOperator(
    task_id='process_xlsx_files',
    python_callable=process_xlsx_files,
    op_kwargs={'keep_local_json': False},
    dag=dag,
)
