import os
from dotenv import load_dotenv 

from helpers.dags_helper import upload_file_to_s3
from queries.staging import QUERY_DROP_GEOGRAPHY_TABLE_STAGING, QUERY_CREATE_EXTERNAL_GEOGRAPHY_TABLE_STAGING

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

PATH_TO_LOCAL_HOME = "/opt/airflow/"
GEOGRAPHY_DATASET_FILE = "worldcities.csv"
GEOGRAPHY_DATASET_URL = "https://simplemaps.com/static/data/world-cities/basic/simplemaps_worldcities_basicv1.77.zip"
GEOGRAPHY_TABLE_NAME = 'geography'

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="geography_data_ingestion_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_geography_dataset_task = BashOperator(
        task_id="download_geography_dataset_task",
        bash_command=(
            f"""
            curl -sSL {GEOGRAPHY_DATASET_URL} -o {PATH_TO_LOCAL_HOME}/{GEOGRAPHY_DATASET_FILE}.zip &&
            unzip -o {PATH_TO_LOCAL_HOME}/{GEOGRAPHY_DATASET_FILE}.zip -d {PATH_TO_LOCAL_HOME}/
            """
        )        
    )
    
    upload_geography_file_to_s3_task = PythonOperator(
        task_id="upload_geography_file_to_s3_task",
        python_callable=upload_file_to_s3,
        op_kwargs={"file": f'{GEOGRAPHY_DATASET_FILE}', "table_name": GEOGRAPHY_TABLE_NAME},
    )
    
    drop_athena_external_table_task = AthenaOperator(
            task_id='drop_geography_external_table_staging',
            query=QUERY_DROP_GEOGRAPHY_TABLE_STAGING,
            database=AWS_ATHENA_STAGING_DATABASE,
            output_location=f"s3://{AWS_BUCKET_NAME}/{GEOGRAPHY_TABLE_NAME}/athena_output/external/drop/",
            workgroup='primary',
            region_name=REGION_NAME
    )
    
    athena_external_table_task = AthenaOperator(
            task_id='create_geography_external_table_staging',
            query=QUERY_CREATE_EXTERNAL_GEOGRAPHY_TABLE_STAGING,
            database=AWS_ATHENA_STAGING_DATABASE,
            output_location=f"s3://{AWS_BUCKET_NAME}/{GEOGRAPHY_TABLE_NAME}/athena_output/external/create/",
            workgroup='primary',
            region_name=REGION_NAME
    )

    download_geography_dataset_task >> upload_geography_file_to_s3_task >> drop_athena_external_table_task >> athena_external_table_task