import os
from dotenv import load_dotenv 
from datetime import datetime, timedelta

from helpers.dags_helper import upload_file_to_s3
from queries.staging import QUERY_CREATE_ICEBERG_PUNCTUALITY_TABLE_STAGING

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
    #start_date=datetime(2024, 1, 1),
    #end_date=datetime(2024, 12, 31),
    #schedule_interval="@monthly",
with DAG(
    dag_id="punctuality_data_ingestion_dag",
    default_args=default_args,
    catchup=True,
    schedule_interval="@monthly",
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_punctuality_dataset_task = BashOperator(
        task_id="download_punctuality_dataset_task",
        bash_command=f"curl -sSL {PUNCTUALITY_DATASET_URL} > {PATH_TO_LOCAL_HOME}/{PUNCTUALITY_DATASET_FILE}"
    )
    
    upload_punctuality_file_to_s3_task = PythonOperator(
        task_id="upload_punctuality_file_to_s3_task",
        python_callable=upload_file_to_s3,
        op_kwargs={"file": PUNCTUALITY_DATASET_FILE, "table_name": PUNCTUALITY_FOLDER_NAME},
    )
    
    create_athena_iceberg_table_task = AthenaOperator(
            task_id='create_punctuality_iceberg_table_staging_task',
            query=QUERY_CREATE_ICEBERG_PUNCTUALITY_TABLE_STAGING,
            database=AWS_ATHENA_STAGING_DATABASE,
            output_location=f"s3://{AWS_BUCKET_NAME}/{PUNCTUALITY_FOLDER_NAME}/athena_output/iceberg/create/",
            workgroup='primary',
            region_name=REGION_NAME
    )
        
    drop_athena_external_table_task = AthenaOperator(
            task_id='drop_punctuality_external_table_staging_task',
            query="""
                DROP TABLE IF EXISTS infrabel_staging_db.punctuality_{{ ds_nodash[:6] }}_ext;
                """,
            database=AWS_ATHENA_STAGING_DATABASE,
            output_location=f"s3://{AWS_BUCKET_NAME}/{PUNCTUALITY_FOLDER_NAME}/athena_output/external/{PUNCTUALITY_TABLE_NAME}/drop/",
            workgroup='primary',
            region_name=REGION_NAME
    )
    
    create_athena_external_table_task = AthenaOperator(
            task_id='create_punctuality_external_table_staging_task',
            query="""
                CREATE EXTERNAL TABLE infrabel_staging_db.punctuality_{{ ds_nodash[:6] }}_ext(
                  DATDEP              string COMMENT 'The departure date',
                  TRAIN_NO            int COMMENT 'The train number',
                  RELATION            string COMMENT 'The train type',
                  TRAIN_SERV          string COMMENT 'The operator',
                  PTCAR_NO            int COMMENT 'The measuring point number',
                  THOP1_COD           string ,
                  LINE_NO_DEP         int COMMENT 'The start line',
                  REAL_TIME_ARR       string COMMENT 'The actual arrival time',
                  REAL_TIME_DEP       string COMMENT 'The actual departure time',
                  PLANNED_TIME_ARR    string COMMENT 'The scheduled time of arrival',
                  PLANNED_TIME_DEP    string COMMENT 'The scheduled time of departure',
                  DELAY_ARR           int COMMENT 'The arrival delay',
                  DELAY_DEP           int COMMENT 'The delayed departure',
                  CIRC_TYP            int ,
                  RELATION_DIRECTION  string COMMENT 'The train direction',
                  PTCAR_LG_NM_NL      string COMMENT 'The name of stopping point',
                  LINE_NO_ARR         int COMMENT 'The arrival line',
                  PLANNED_DATE_ARR    string COMMENT 'The estimated date of arrival',
                  PLANNED_DATE_DEP    string COMMENT 'The scheduled departure date',
                  REAL_DATE_ARR       string COMMENT 'The actual arrival date',
                  REAL_DATE_DEP       string COMMENT 'The actual departure date'
                )
                ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
                WITH SERDEPROPERTIES ('field.delim' = ',', 'skip.header.line.count'='1')
                STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
                LOCATION 's3://infrabel-bucket/punctuality/csv/'
                TBLPROPERTIES ('classification' = 'csv');
                """,
            database=AWS_ATHENA_STAGING_DATABASE,
            output_location=f"s3://{AWS_BUCKET_NAME}/{PUNCTUALITY_FOLDER_NAME}/athena_output/external/{PUNCTUALITY_TABLE_NAME}/create/",
            workgroup='primary',
            region_name=REGION_NAME
    )

    add_unque_id_to_external_table_task = AthenaOperator(
            task_id='add_unque_id_to_punctuality_external_table_staging_task',
            query="""
                CREATE TABLE infrabel_staging_db.punctuality_{{ ds_nodash[:6] }}
                WITH (
                  format = 'PARQUET',
                  external_location = 's3://infrabel-bucket/punctuality/tables/parquet/punctuality_{{ ds_nodash[:6] }}/'
                ) AS
                SELECT
                  md5(to_utf8(concat(
                    coalesce(cast(DATDEP as varchar), ''),
                    coalesce(cast(TRAIN_NO as varchar), ''),
                    coalesce(cast(RELATION as varchar), ''),
                    coalesce(cast(PTCAR_NO as varchar), ''),
                    coalesce(cast(PTCAR_LG_NM_NL as varchar), '')
                  ))) AS unique_row_id,
                  'punctuality_{{ ds_nodash[:6] }}.csv'AS filename,
                  *
                FROM infrabel_staging_db.punctuality_{{ ds_nodash[:6] }}_ext;
                """,
            database=AWS_ATHENA_STAGING_DATABASE,
            output_location=f"s3://{AWS_BUCKET_NAME}/{PUNCTUALITY_FOLDER_NAME}/athena_output/parquet/{PUNCTUALITY_FOLDER_NAME}/create/",
            workgroup='primary',
            region_name=REGION_NAME
    )   

    insert_into_iceberg_table_task = AthenaOperator(
            task_id='insert_into_punctuality_iceberg_table_task',
            query="""
                INSERT INTO infrabel_staging_db.punctuality_iceberg
                SELECT
                  S.unique_row_id,
                  S.filename,
                  S.DATDEP,
                  S.TRAIN_NO,
                  S.RELATION,
                  S.TRAIN_SERV,
                  S.PTCAR_NO,
                  S.THOP1_COD,
                  S.LINE_NO_DEP,
                  S.REAL_TIME_ARR,
                  S.REAL_TIME_DEP,
                  S.PLANNED_TIME_ARR,
                  S.PLANNED_TIME_DEP,
                  S.DELAY_ARR,
                  S.DELAY_DEP,
                  S.CIRC_TYP,
                  S.RELATION_DIRECTION,
                  S.PTCAR_LG_NM_NL,
                  S.LINE_NO_ARR,
                  S.PLANNED_DATE_ARR,
                  S.PLANNED_DATE_DEP,
                  S.REAL_DATE_ARR,
                  S.REAL_DATE_DEP
                FROM infrabel_staging_db.punctuality_{{ ds_nodash[:6] }} S
                LEFT JOIN  infrabel_staging_db.punctuality_iceberg T
                  ON T.unique_row_id = S.unique_row_id
                WHERE T.unique_row_id IS NULL;
                """,
            database=AWS_ATHENA_STAGING_DATABASE,
            output_location=f"s3://{AWS_BUCKET_NAME}/{PUNCTUALITY_FOLDER_NAME}/athena_output/parquet/{PUNCTUALITY_FOLDER_NAME}/create/",
            workgroup='primary',
            region_name=REGION_NAME
    )

    download_punctuality_dataset_task >> upload_punctuality_file_to_s3_task >> create_athena_iceberg_table_task >> drop_athena_external_table_task  >> create_athena_external_table_task >> add_unque_id_to_external_table_task >> insert_into_iceberg_table_task