import os
from dotenv import load_dotenv 
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# loading variables from .env file
load_dotenv(dotenv_path="/opt/airflow/.env")

AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")

PATH_TO_LOCAL_HOME = "/opt/airflow/"

def upload_file_to_s3(file: str, table_name: str, **kwargs):
    s3_hook = S3Hook()
    file_path = os.path.join(PATH_TO_LOCAL_HOME, file)
    s3_key = f"{table_name}/csv/{file}"
    print("key", s3_key)
    s3_hook.load_file(filename=file_path, key=s3_key, bucket_name=AWS_BUCKET_NAME, replace=True)
    s3_url = f"s3://{AWS_BUCKET_NAME}/{s3_key}"
    return s3_url