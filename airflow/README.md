# Infrabel Train Punctuality Project


## Run Airflow

From the project root folder, move to the ./airflow directory.
Create a couple of folders needed for the setup : dags, logs, plugins and config.

    cd airflow

Danload the offixial docker-compose [file](https://airflow.apache.org/docs/apache-airflow/2.10.5/docker-compose.yaml). you can also set `AIRFLOW__CORE__LOAD_EXAMPLES` to `false`

Create environment variables in the .env file for our future Docker containers.

Fill in the content of the .env file. The value for AIRFLOW_UID is obtained from the following command:

    echo -e "AIRFLOW_UID=$(id -u)"

Then the value for AIRFLOW_GID can be left to 0.

1. Initialize everything    

````
docker compose up airflow-init
````

2. Run airflow

````
docker compose up
````
    
3. Add extra packages

Create a `Dockerfile` and a `requirement.txt` files in the directory, her we want to add `apache-airflow-providers-amazon` and 
`apache-airflow-providers-dbt-cloud`  

Here is the Docker file:

````
FROM apache/airflow:2.10.5

ENV AIRFLOW_HOME=/opt/airflow

USER root

RUN apt-get update \
&& apt-get install -y --no-install-recommends \
        unzip\
&& apt-get autoremove -yqq --purge \
&& apt-get clean \
&& rm -rf /var/lib/apt/lists/*

USER airflow
COPY .env .
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
````

Here is the requirement.txt file:

````
apache-airflow-providers-amazon
apache-airflow-providers-dbt-cloud
pyarrow
````

Lets build the image from dockerfile using 

    docker build -f Dockerfile -t custom-airflow .

    docker compose up -d

Big thanks to 
[Shantanu Khond](https://www.youtube.com/watch?v=AQuYwu2WolQ)

### A Downloading Datasets from Source (Airflow + Python)
1. Define your Airflow DAG for dataset download.
2. Use BashOperators or PythonOperators within your DAG to download raw CSV files from the Infrabel source.
3. Verify that the datasets are stored locally in the designated folder.

You can use the Airflow UI to trigger the data ingestion DAGs only fo the station dag. For the punctuality dag you must run it on the terminal: 

     docker container ps
     docker exec -it <your_airflow_worker_container_id> airflow dags backfill -s <start_date> -e <end_date> punctuality_data_ingestion_dag

where the dates are in the following format `YYYY-MM-DD`. We have to use the terminal because we need to use a backfill to load data that are in the past.

### B Uploading Parquet Files into the Datalake (Airflow + Python)
1. Create an Airflow DAG task to convert the downloaded CSV files to Parquet (using Python with libraries like Pandas/pyarrow).
2. Upload the Parquet files to the designated S3 bucket using an S3 operator or a custom Python function.
3. Confirm that the files are available in S3.


punctuality_data_ingestion_dag Graph
![punctuality_data_ingestion_dag](/screenshots/punctuality_dag.png "punctuality_data_ingestion_dag")

station_data_ingestion_dag Graph
![station_data_ingestion_dag](/screenshots/station_dag.png "station_data_ingestion_dag")

### C Creating Tables and Loading Data (Airflow + Python)
1. Create external tables in Athena that point to the S3 locations where your data resides.
2. Use Airflow DAG tasks (AthenaOperator) to execute SQL queries for table creation.
3. Check Athena for successful table creation and data availability.
