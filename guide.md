# Infrabel Train Punctuality Project
<br>

## Overview
[A. Project Architecture](#a-project-architecture)  
[B. Step-by-Step Guide to Run the Project](#b-step-by-step-guide-to-run-the-project)  
 [1. Creating Cloud Infrastructure with Terraform](#1-creating-cloud-infrastructure-with-terraform)  
[2. Containerization with Docker](#2-containerization-with-docker)  
[3. Run Airflow](#3-run-airflow)  
[4. Transformation, Modeling, and Generalization of Data with dbt](#4-transformation-modeling-and-generalization-of-data-with-dbt)  
[5. Creating Reports and Dashboard Templates with Power BI](#5-creating-reports-and-dashboard-templates-with-power-bi)  
  

<br>


## A. Project Architecture

1. **Cloud Infrastructure (Terraform):**  
   - Create S3 buckets for raw and transformed data.
   - Provision Athena databases for staging and data warehouse layers.

2. **Containerization (Docker):**  
   - Build Docker containers that include Python, Airflow, and necessary libraries.
   - Ensure that the environment is reproducible and consistent across development and production.

3. **Data Ingestion (Airflow + Python):**  
   - Download raw datasets from the Infrabel source.
   - Convert CSV files to Parquet format for efficient querying.
   - Upload the Parquet files into the S3 datalake.

4. **Table Creation (Airflow + Python):**  
   - Use Airflow tasks to create external tables in Athena that point to data in S3.
   - Load data from S3 into these tables.

5. **Data Transformation (dbt):**  
   - Transform, model, and generalize data into a final data warehouse schema using dbt.
   - Build dimension and fact tables (e.g., DimDate, DimTrain, DimStation, FactTrain) that support efficient analysis.

6. **Reporting (Power BI):**  
   - Develop report layouts and dashboards in Power BI.
   - Connect Power BI to Athena to pull data from the data warehouse and create interactive visualizations.

<br>

## B. Step-by-Step Guide to Run the Project

### Prerequisites
- **AWS Account:** Access to AWS with permissions to create S3 buckets, Athena databases, etc.
- **Terraform Installed:** To provision cloud infrastructure.
- **Docker Installed:** To build and run containerized services.
- **Airflow Installed (via Docker):** For orchestrating workflows.
- **dbt Cloud or dbt Core Setup:** For data transformation and modeling.
- **Power BI Desktop:** For developing interactive dashboards.
- **Environment Variables:** Create a `.env` file with variables such as:
  - `AWS_BUCKET_NAME`
  - `AWS_ATHENA_STAGING_DATABASE`
  - `REGION_NAME`
  - `DBT_ACCOUNT_ID`, `DBT_API_TOKEN`, `DBT_PROJECT_NAME`, `DBT_JOB_ID`, `DBT_JOB_ENVIRONMENT`, etc.

### 1. Creating Cloud Infrastructure with Terraform
1. Clone the repository.
2. Navigate to the Terraform directory.
3. Update the `variables.tf` file with your AWS settings.
4. Run the following commands:
   ```bash
   terraform init
   terraform plan
   terraform apply
   ```
   This will create the S3 bucket and Athena databases required for staging and warehousing.

### 2. Containerization with Docker
1. Ensure your Dockerfile includes Airflow, Python, and other necessary libraries.
2. Build your Docker image:
   ```bash
   docker build -t infrabel-project .
   ```
3. Start the containers (or use docker-compose if provided) to launch the Airflow webserver, scheduler, and workers.

### 3. Run Airflow
You can follow the guide [here](/airflow/README.md)

![punctuality_data_ingestion_dag](/screenshots/punctuality_dag.png "punctuality_data_ingestion_dag")

### 4. Transformation, Modeling, and Generalization of Data (with dbt
In this step, we transform the raw data from our staging area into structured, analytics-ready tables using dbt. Our approach involves:

1. Data Cleansing and Casting:
We use Common Table Expressions (CTEs) to safely cast raw fields (such as dates and times) and clean up data from the staging layer. For instance, the safe_cast CTE transforms fields from the staging table into their proper data types.

2. Surrogate Key Generation:
We generate a surrogate key for our fact table using a combination of relevant columns (like train number, departure date, measuring point, etc.) via the dbt_utils.generate_surrogate_key macro. This key ensures each fact row is uniquely identified.

3. Partitioning:
In our dbt configuration, we partition the table on the field `FK_Dim_Date_dep`, which is computed as an integer in the `YYYYMMDD` format. Partitioning allows AWS Athena to efficiently prune partitions when filtering queries by date, greatly improving query performance.

```
{{
    config(
        materialized='table',
        partition_by={
          "field": "FK_Dim_Date_dep",
          "data_type": "int",
          "granularity": "day"
        }
    )
}}
```

4. Clustering:
Clustering is a technique used in some databases to physically order data within partitions to further improve query performance. However, for Iceberg table formats in AWS Athena, clustering is not supported. Therefore, our model does not include any clustering. This is expected behavior—our focus is solely on partitioning, which is fully supported and effective for our use case.

5. Generalization:
The transformed data is then loaded into a fact table that can be easily joined with various dimension tables (e.g., DimDate, DimTrain, DimStation). This enables a flexible, analytical data model that supports a variety of upstream queries.

Lineage graph:

![Lineage graph](/screenshots/dbt%20lineage.png "Lineage graph")

DBT Documentation

![Docs graph](/screenshots/dbt%20docs.png "Documentation graph")

### 5. Creating Reports and Dashboard Templates with Power BI


You can follow the guide [here](/powerbi.md)

![Dashboard](/screenshots/dashbord%2001.png "Dashboard")

<br>


## Running the Project

1. **Provision Infrastructure:**  
   Run Terraform to create your AWS resources.
2. **Start Containers:**  
   Build and run your Docker containers for Airflow and other tools.
3. **Trigger Airflow DAGs:**  
   Use the Airflow UI to trigger the data ingestion DAGs.
4. **Monitor Data Transformation:**  
   Execute and monitor dbt jobs.
5. **Develop Power BI Reports:**  
   Connect to Athena and build your dashboards.

