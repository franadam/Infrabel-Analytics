import os
from dotenv import load_dotenv 

from helpers.dags_helper import upload_file_to_s3
from queries.staging import QUERY_DROP_STATION_TABLE_STAGING, QUERY_CREATE_EXTERNAL_STATION_TABLE_STAGING, QUERY_DROP_STOP_TABLE_STAGING, QUERY_CREATE_EXTERNAL_STOP_TABLE_STAGING

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

# loading variables from .env file
load_dotenv(dotenv_path="/opt/airflow/.env")

AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_ATHENA_STAGING_DATABASE = os.getenv("AWS_ATHENA_STAGING_DATABASE")
REGION_NAME = os.getenv("REGION_NAME")

PATH_TO_LOCAL_HOME = "/opt/airflow"
STATION_DATASET_FILE = "stations.csv"
STATION_TABLE_NAME = 'station'
STATION_DATASET_URL = "https://github.com/iRail/stations/blob/c1967bff18088b45b6be369aba3faf96d4886f36/stations.csv"
IRAIL_DATASET_URL = "https://github.com/iRail/stations/archive/refs/tags/2.0.10.tar.gz"
STOP_DATASET_URL = "https://github.com/iRail/stations/blob/c1967bff18088b45b6be369aba3faf96d4886f36/stops.csv"
STOP_DATASET_FILE = "stops.csv"
STOP_TABLE_NAME = 'stop'

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="station_data_ingestion_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de', 'ingestion'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_station_dataset_task",
        bash_command=(
            f"""
            mkdir -p {PATH_TO_LOCAL_HOME}/{STATION_TABLE_NAME} && curl -sSL {IRAIL_DATASET_URL} | tar -xz --strip-components=1 -C {PATH_TO_LOCAL_HOME}/{STATION_TABLE_NAME}/
            """
        )        
    )
    
    upload_station_file_to_s3_task = PythonOperator(
        task_id="upload_station_file_to_s3_task",
        python_callable=upload_file_to_s3,
        op_kwargs={"file": f'{STATION_DATASET_FILE}', "path": f"{PATH_TO_LOCAL_HOME}/{STATION_TABLE_NAME}","table_name": STATION_TABLE_NAME},
    )
    
    upload_stop_file_to_s3_task = PythonOperator(
        task_id="upload_stop_file_to_s3_task",
        python_callable=upload_file_to_s3,
        op_kwargs={"file": f'{STOP_DATASET_FILE}', "path": f"{PATH_TO_LOCAL_HOME}/{STATION_TABLE_NAME}", "table_name": STOP_TABLE_NAME},
    )
    
    drop_station_external_table_task = AthenaOperator(
            task_id='drop_station_external_table_staging',
            query=QUERY_DROP_STATION_TABLE_STAGING,
            database=AWS_ATHENA_STAGING_DATABASE,
            output_location=f"s3://{AWS_BUCKET_NAME}/{STATION_TABLE_NAME}/athena_output/external/drop/",
            workgroup='primary',
            region_name=REGION_NAME
    )
    
    create_station_external_table_task = AthenaOperator(
            task_id='create_station_external_table_staging',
            query=QUERY_CREATE_EXTERNAL_STATION_TABLE_STAGING,
            database=AWS_ATHENA_STAGING_DATABASE,
            output_location=f"s3://{AWS_BUCKET_NAME}/{STATION_TABLE_NAME}/athena_output/external/create/",
            workgroup='primary',
            region_name=REGION_NAME
    )
        
    drop_stop_external_table_task = AthenaOperator(
            task_id='drop_stop_external_table_staging',
            query=QUERY_DROP_STOP_TABLE_STAGING,
            database=AWS_ATHENA_STAGING_DATABASE,
            output_location=f"s3://{AWS_BUCKET_NAME}/{STOP_TABLE_NAME}/athena_output/external/drop/",
            workgroup='primary',
            region_name=REGION_NAME
    )
    
    create_stop_external_table_stask = AthenaOperator(
            task_id='create_stop_external_table_staging',
            query=QUERY_CREATE_EXTERNAL_STOP_TABLE_STAGING,
            database=AWS_ATHENA_STAGING_DATABASE,
            output_location=f"s3://{AWS_BUCKET_NAME}/{STOP_TABLE_NAME}/athena_output/external/create/",
            workgroup='primary',
            region_name=REGION_NAME
    )

    download_dataset_task >> upload_station_file_to_s3_task >> upload_stop_file_to_s3_task >> drop_station_external_table_task >> create_station_external_table_task >> drop_stop_external_table_task >> create_stop_external_table_stask
