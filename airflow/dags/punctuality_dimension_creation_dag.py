import os
from dotenv import load_dotenv 

from queries.dw import QUERY_CREATE_DW_DIM_DATE_TABLE, QUERY_CREATE_DW_DIM_TIME_TABLE

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

# loading variables from .env file
load_dotenv(dotenv_path="/opt/airflow/.env")

AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_ATHENA_STAGING_DATABASE = os.getenv("AWS_ATHENA_STAGING_DATABASE")
REGION_NAME = os.getenv("REGION_NAME")

PATH_TO_LOCAL_HOME = "/opt/airflow/"
PUNCTUALITY_FOLDER_NAME = 'punctuality'
PUNCTUALITY_DATASET_FILE = "punctuality_{{ ds_nodash[:6] }}.csv"
PUNCTUALITY_TABLE_NAME = "punctuality_{{ds_nodash[:6]}}"
PUNCTUALITY_DATASET_URL = "https://fr.ftp.opendatasoft.com/infrabel/PunctualityHistory/Data_raw_punctuality_{{ ds_nodash[:6] }}.csv"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="punctuality_dimension_creation_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:
    
    create_date_dimension_table_task = AthenaOperator(
            task_id='create_date_dimension_table_task',
            query=QUERY_CREATE_DW_DIM_DATE_TABLE,
            database=AWS_ATHENA_STAGING_DATABASE,
            output_location=f"s3://{AWS_BUCKET_NAME}/{PUNCTUALITY_TABLE_NAME}/athena_output/parquet/dim/date/",
            workgroup='primary',
            region_name=REGION_NAME
    
    create_time_dimension_table_task = AthenaOperator(
            task_id='create_time_dimension_table_task',
            query=QUERY_CREATE_DW_DIM_TIME_TABLE,
            database=AWS_ATHENA_STAGING_DATABASE,
            output_location=f"s3://{AWS_BUCKET_NAME}/{PUNCTUALITY_TABLE_NAME}/athena_output/parquet/dim/time/",
            workgroup='primary',
            region_name=REGION_NAME
    )

    create_date_dimension_table_task >> create_time_dimension_table_task