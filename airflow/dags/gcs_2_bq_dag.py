import os

from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "trips_data_all")

def get_file_list(colour, ti):
  hook = GCSHook()
  list = hook.list(
    bucket_name=BUCKET
  )
  
  file_list = []

  for file in list:
    print(file)
    if file.startswith(f"{colour}/{colour}") and file.endswith('.parquet'):
      file_list.append(file)
  ti.xcom_push(key=f'file_list_{colour}', value=file_list)

def create_query(colour, ds_col, ti):
  files = ti.xcom_pull(key=f'file_list_{colour}', task_ids=f'get_gcs_file_list_{colour}')
  QUERY_P1 = f"""
            DROP TABLE IF EXISTS polynomial-land.trips_data_all.temp_table_{colour}; 
            LOAD DATA INTO polynomial-land.trips_data_all.temp_table_{colour} 
            FROM FILES ( 
              format = 'PARQUET', 
              uris = ['gs://{BUCKET}/"""
  QUERY_P2 = f"""'] 
            );

            INSERT INTO polynomial-land.trips_data_all.{colour}_trip_table(
              VendorID
              ,{ds_col['pickup']}
              ,{ds_col['dropoff']}
              ,passenger_count
              ,trip_distance
              ,RatecodeID
              ,store_and_fwd_flag
              ,PULocationID
              ,DOLocationID
              ,payment_type
              ,fare_amount
              ,extra
              ,mta_tax
              ,tip_amount
              ,tolls_amount
              ,improvement_surcharge
              ,total_amount
              ,congestion_surcharge
              ,airport_fee
              )
                SELECT 
                  VendorID
                  ,{ds_col['pickup']}
                  ,{ds_col['dropoff']}
                  ,passenger_count
                  ,trip_distance
                  ,RatecodeID
                  ,store_and_fwd_flag
                  ,PULocationID
                  ,DOLocationID
                  ,payment_type
                  ,fare_amount
                  ,extra
                  ,mta_tax
                  ,tip_amount
                  ,tolls_amount
                  ,improvement_surcharge
                  ,total_amount
                  ,congestion_surcharge
                  {ds_col['airport']}
            FROM polynomial-land.trips_data_all.temp_table_{colour};
  """
  COMBINED_QUERY=""
  for file in files:
    COMBINED_QUERY+=QUERY_P1 + file + QUERY_P2
  
  ti.xcom_push(key=f"{colour}_query", value=COMBINED_QUERY)

COLOUR_RANGE = {
  'yellow': {
    'pickup': 'tpep_pickup_datetime',
    'dropoff': 'tpep_dropoff_datetime',
    'airport': ',CAST(airport_fee AS FLOAT64) as airport_fee'},
  'green': {
    'pickup': 'lpep_pickup_datetime',
    'dropoff': 'lpep_dropoff_datetime',
    'airport': ',NULL as airport_fee'}
    }

default_args = {
  "owner": "airflow",
  "start_date": days_ago(1),
  "depends_on_past": False,
  "retries": 0,
}

with DAG(
  dag_id="gcs_2_bq_dag",
  schedule_interval="@daily",
  default_args=default_args,
  catchup=False,
  max_active_runs=3,
  tags=['dtc-de'],
) as dag:

  for colour, ds_col in COLOUR_RANGE.items():
    gcs_2_gcs_task = GCSToGCSOperator(
      task_id=f"gcs_to_gcs_{colour}_task",
      source_bucket=BUCKET,
      source_objects=[f'raw2/{colour}/*'],
      destination_bucket=BUCKET,
      destination_object=f"{colour}/",
      move_object=False
    )

    get_gcs_file_list = PythonOperator(
      task_id=f"get_gcs_file_list_{colour}",
      python_callable=get_file_list,
      op_kwargs={"colour": colour}
    )

    create_bq_query = PythonOperator(
      task_id=f"create_bq_query_{colour}",
      python_callable=create_query,
      op_kwargs={"colour": colour, "ds_col": ds_col}
    )

    gcs_2_bq_empty_table_task = BigQueryCreateEmptyTableOperator(
        task_id=f"gcs_2_bq_empty_table_task_{colour}",
        project_id=PROJECT_ID,
        dataset_id=BIGQUERY_DATASET,
        table_id=f"{colour}_trip_table",
        schema_fields=[
            {"name": "VendorID", "type":	"INTEGER", "mode":	"NULLABLE"},
            {"name": ds_col['pickup'], "type":	"TIMESTAMP",	"mode":	"NULLABLE"},
            {"name": ds_col['dropoff'],	"type": "TIMESTAMP", "mode":	"NULLABLE"},
            {"name": "store_and_fwd_flag", "type":	"STRING",	"mode":	"NULLABLE"},
            {"name": "RatecodeID", "type": "FLOAT",	"mode":	"NULLABLE"},
            {"name": "PULocationID", "type": "INTEGER",	"mode":	"NULLABLE"},
            {"name": "DOLocationID", "type":"INTEGER",	"mode":	"NULLABLE"},
            {"name": "passenger_count", "type":"FLOAT",	"mode":	"NULLABLE"},
            {"name": "trip_distance", "type":"FLOAT",	"mode":	"NULLABLE"},
            {"name": "fare_amount", "type": "FLOAT",	"mode":	"NULLABLE"},
            {"name": "extra", "type": "FLOAT",	"mode":	"NULLABLE"},
            {"name": "mta_tax", "type":"FLOAT",	"mode":	"NULLABLE"},
            {"name": "tip_amount", "type":"FLOAT",	"mode":	"NULLABLE"},
            {"name": "tolls_amount", "type":"FLOAT",	"mode":	"NULLABLE"},
            {"name": "ehail_fee",	"type":"INTEGER",	"mode":	"NULLABLE"},
            {"name": "improvement_surcharge", "type":	"FLOAT",	"mode":	"NULLABLE"},
            {"name": "total_amount", "type":"FLOAT",	"mode":	"NULLABLE"},
            {"name": "payment_type", "type":"INTEGER",	"mode":	"NULLABLE"},
            {"name": "trip_type",	"type":"FLOAT",	"mode":	"NULLABLE"},
            {"name": "congestion_surcharge",	"type": "FLOAT", "mode": "NULLABLE"},
            {"name": "airport_fee",	"type": "FLOAT", "mode": "NULLABLE"}
        ]    
      )

    gcs_2_bq_populate_table = BigQueryInsertJobOperator(
      task_id=f"gcs_2_bq_populate_table_{colour}",
      configuration={
        "query": {
          "query": f"{{{{ task_instance.xcom_pull(task_ids='create_bq_query_{colour}', key='{colour}_query') }}}}",
          "useLegacySql": False,
        }
      }
    )

    CREATE_PART_TBL_QUERY = (
      f"CREATE OR REPLACE TABLE {PROJECT_ID}.{BIGQUERY_DATASET}.{colour}_trip_data_partitioned \
      PARTITION BY \
      DATE({ds_col['pickup']}) AS \
      SELECT * FROM {PROJECT_ID}.{BIGQUERY_DATASET}.{colour}_trip_table"
    )

    bq_2_part_task = BigQueryInsertJobOperator(
      task_id=f"bq_2_part_task_{colour}",
      configuration={
        "query": {
          "query": CREATE_PART_TBL_QUERY,
          "useLegacySql": False,
        }
      }
    )

    gcs_2_gcs_task >> get_gcs_file_list >> create_bq_query >> gcs_2_bq_empty_table_task >> gcs_2_bq_populate_table >> bq_2_part_task