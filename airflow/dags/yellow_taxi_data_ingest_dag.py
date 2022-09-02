import os

from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

# Below line used if wanting to pass in custom date variable, using other way for course homework
# dataset_file = "yellow_tripdata_{{ dag_run.conf['date'] }}.parquet"
# dataset_file = "yellow_tripdata_{{ execution_date.strftime('%Y-%m') }}.parquet"
# dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}"

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "trips_data_all")

def upload_to_gcs(bucket, object_name, local_file):
  """
  Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
  """
  # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
  # (Ref: https://github.com/googleapis/python-storage/issues/74)
  storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
  storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
  # End of Workaround

  client = storage.Client()
  bucket = client.bucket(bucket)

  blob = bucket.blob(object_name)
  blob.upload_from_filename(local_file)

default_args = {
  "owner": "airflow",
  "start_date": days_ago(1),
  "depends_on_past": False,
  "retries": 0,
}

URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data'

YELLOW_TAXI_URL_TEMPLATE = URL_PREFIX + "/yellow_tripdata_{{ dag_run.conf['date'] }}.parquet"
YELLOW_TAXI_FILE_TEMPLATE = AIRFLOW_HOME + "/yellow_tripdata_{{ dag_run.conf['date'] }}.parquet"
YELLOW_TAXI_GCS_PATH_TEMPLATE = "raw/yellow_tripdata/{{ dag_run.conf['date'] }}/yellow_tripdata_{{ dag_run.conf['date'] }}.parquet"
YELLOW_TAXI_TABLE_NAME_TEMPLATE = "yellow_tripdata_{{ dag_run.conf['date'] }}"

# DAG declaration using a context manager
with DAG(
  dag_id="yellow_taxi_data_ingest",
  schedule_interval="0 1 1 1 *",
  default_args=default_args,
  start_date=datetime(2019, 1, 1),
  catchup=True,
  max_active_runs=3,
  tags=['dtc-de'],
) as yellow_taxi_data_ingest:

  download_data_set_task = BashOperator(
    task_id="download_dataset_task",
    bash_command=f"curl -sSLf {YELLOW_TAXI_URL_TEMPLATE} > {YELLOW_TAXI_FILE_TEMPLATE}"
  )

  local_to_gcs_task = PythonOperator(
    task_id="local_to_gcs_task",
    python_callable=upload_to_gcs,
    op_kwargs={
      "bucket": BUCKET,
      "object_name": YELLOW_TAXI_GCS_PATH_TEMPLATE,
      "local_file": YELLOW_TAXI_FILE_TEMPLATE,
    },
  )

  bigquery_external_table_task = BigQueryCreateExternalTableOperator(
    task_id="bigquery_external_table_task",
    table_resource={
      "tableReference": {
        "projectId": PROJECT_ID,
        "datasetId": BIGQUERY_DATASET,
        "tableId": YELLOW_TAXI_TABLE_NAME_TEMPLATE,
      },
      "externalDataConfiguration": {
        "sourceFormat": "PARQUET",
        "sourceUris": [f"gs://{BUCKET}/{YELLOW_TAXI_GCS_PATH_TEMPLATE}"],
      },
    },
  )

  download_data_set_task >> local_to_gcs_task >> bigquery_external_table_task