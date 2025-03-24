import os
from dotenv import load_dotenv 

from helpers.dags_helper import upload_file_to_s3

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# loading variables from .env file
load_dotenv(dotenv_path="/opt/airflow/.env")

AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")

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
    dag_id="data_ingestion_dag",
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
    
    download_geography_dataset_task >> upload_geography_file_to_s3_task 