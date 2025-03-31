# Train Punctuality Analysis on the Belgian Railway Network
<br>

## Overview
[1. Problem Description](#1-problem-description)  
[2. Data Sources and Dictionary](#2-data-sources-and-dictionary)  
[3. Infrastructure](#3-infrastructure)  
[4. Project Pipeline](#4-project-pipeline)   
[5. Future Improvements](#5-future-improvements)  
[6. Acknowledgements](#6-acknowledgements)  

<br>



## 1. Problem Description

The primary goal of this project is to measure and analyze the punctuality of trains across the entire Belgian railway network. With increasing pressure on public transport efficiency and customer satisfaction, understanding train delays has become critical. This project addresses several key questions:

- **What is the overall on-time performance of the trains?**  
  By analyzing arrival and departure delays, we aim to provide a clear picture of the network’s punctuality.

- **What is the delay distribution by time of day?**  
  The analysis will uncover patterns in delay behavior over different times of day.

- **What is the number of train events (arrivals or departures) with a delay greater than 5 minutes for each station?**  
  It shows the delay frequency per station.

Overall, this project provides actionable insights to optimize train schedules, improve passenger experience, and guide infrastructure investments.

<br>


## 2. Data Sources and Dictionary

### Data Sources

**Infrabel Punctuality Data:**  
- **Source:** The raw punctuality data is provided by Infrabel and is available monthly from January 2024.
- **Link:** [Infrabel Punctuality History](https://opendata.infrabel.be/explore/dataset/stiptheid-gegevens-maandelijksebestanden/information/?sort=mois&refine.mois=2024)
- **Description:** These files contain details about train arrivals and departures, including scheduled and actual times, delays (in seconds), train number, operator, measuring point, and more.

**iRail Station Data:**  
- **Source:** Station data is sourced from iRail.
- **Link:** [iRail Stations CSV](https://github.com/iRail/stations/blob/c1967bff18088b45b6be369aba3faf96d4886f36/stations.csv)
- **Description:** This dataset provides metadata on train stations such as station names, alternative names in various languages, geographic coordinates, and additional attributes like official transfer times and stop frequencies.

### Data Dictionary

**Raw Punctuality Data**

- `DATDEP (string):` The departure date.
- `TRAIN_NO (int):` The train number.
- `RELATION (string):` The type of train (e.g., regional, intercity).
- `TRAIN_SERV (string):` The operator of the train.
- `PTCAR_NO (int):` The measuring point number.
- `THOP1_COD (string):` Additional code or indicator.
- `LINE_NO_DEP (int):` The departure line.
- `REAL_TIME_ARR (string):` The actual arrival time.
- `REAL_TIME_DEP (string):` The actual departure time.
- `PLANNED_TIME_ARR (string):` The scheduled arrival time.
- `PLANNED_TIME_DEP (string):` The scheduled departure time.
- `DELAY_ARR (int):` The delay at arrival (in seconds).
- `DELAY_DEP (int):` The delay at departure (in seconds).
- `CIRC_TYP (int):` A code indicating a specific characteristic (e.g., type of circuit).
- `RELATION_DIRECTION (string):` The train’s direction, typically containing two station names separated by ' -> '.
- `PTCAR_LG_NM_NL (string):` The name of the stopping point.
- `LINE_NO_ARR (int):` The arrival line.
- `PLANNED_DATE_ARR (string):` The estimated date of arrival.
- `PLANNED_DATE_DEP (string):` The scheduled departure date.
- `REAL_DATE_ARR (string):` The actual arrival date.
- `REAL_DATE_DEP (string):` The actual departure date.

**Raw Station Data**
- `URI (string):` The URI providing additional information about the station (e.g., real-time departures); includes the NMBS/SNCB station ID.
- `name (string):` The primary name of the station, chosen for neutrality (e.g., French name for Wallonia, bilingual for Brussels, Dutch for Flanders).
- `alternative-fr (string):` Alternative name in French, if available.
- `alternative-nl (string):` Alternative name in Dutch, if available.
- `alternative-de (string):` Alternative name in German, if available.
- `alternative-en (string):` Alternative name in English, if available.
- `taf-tap-code (string):` Code representing the country for the station.
- `telegraph-code (string):` Another country code associated with the station.
- `country-code (string):` Country code.
- `longitude (string):` The station's longitude.
- `latitude (string):` The station's latitude.
- `avg_stop_times (string):` Computed field representing the average number of vehicles stopping at the station per day.
- `official_transfer_time (string):` Computed field indicating the official transfer time according to NMBS/SNCB.
<br>



## 3. Infrastructure

This project is fully developed and deployed in the cloud, leveraging a modern data engineering stack to ensure scalability, reliability, and ease of maintenance. Key cloud components and IaC tools include:

- **AWS Cloud Infrastructure:**  
  - **Amazon S3:** Serves as the data lake, storing raw and processed data files.
  - **AWS Athena:** Used for interactive SQL querying directly on the data stored in S3.

- **Infrastructure as Code (IaC):**  
  - **Terraform:** All cloud resources (such as S3 buckets and Athena databases) are provisioned using Terraform. This ensures that the infrastructure is version-controlled, reproducible, and easily scalable.

- **Orchestration and Data Pipeline Management:**  
  - **Containerization (Docker):**   Build Docker containers that include Python, Airflow, and necessary libraries. Ensure that the environment is reproducible and consistent across development and production.
  - **Apache Airflow:** Manages end-to-end data ingestion pipelines, coordinating tasks such as data download, transformation, and loading into the data warehouse.
  - **dbt (Data Build Tool):** Transforms raw data into an analytics-ready format, enabling robust data modeling in a structured and version-controlled manner.

Together, these cloud-based components ensure that our project is built on a scalable and reliable infrastructure that can handle high volumes of data and complex analytics workflows, all while maintaining reproducibility and ease of deployment.

<br>


## 4. Project Pipeline

The end-to-end pipeline is built using a modern tech stack and follows these key stages:

1. Creating Cloud infrastructure:  S3 bucket, Athena databases (Terraform)
2. Containerization: Creating containers with the necessary tools: python, Airflow , etc (Docker)
3. Downloading datasets from source (Airflow + python)
4. Uploading parquet files into the datalake S3 (Airflow + python)
5. Creating tables and uploaded data into them from S3 (Airflow+ python)
6. Transformation, modeling and generalization of data into a database (dbt)
7. Creation report and dashboard template and filling it with data from datawarehouse (Power BI)

![Pipeline](/screenshots/architecture.png "Pipeline")

You can use the step by step [note](guide.md) to reproduce the project.

<br>



## 5. Future Improvements

- **Distributed Processing with Spark:**  
  Integrate Apache Spark into the data pipeline to preprocess raw files in a distributed manner. This will significantly speed up data transformation tasks, enable efficient handling of large datasets, and improve overall scalability.

- **Real-Time Stream Processing:**  
  Implement stream processing (using technologies such as Apache Kafka and Spark Structured Streaming or Apache Flink) to capture and process live punctuality data. This enhancement will allow continuous monitoring of train performance and provide up-to-date insights for operational decision-making.

- **Full Integration of dbt Pipelines:**  
  Currently, the dbt transformations are run separately from Airflow. By incorporating dbt pipelines directly into Airflow DAGs, along with Spark transformation tasks, the entire ETL process will become fully automated. This integration will improve the reliability, maintainability, and scheduling of data workflows, ensuring that the data warehouse is always up-to-date.

<br>


## 6. Acknowledgements

I would like to express my sincere gratitude to DataTalks Club for creating this comprehensive Data Engineering course, which has empowered learners to acquire essential data engineering skills completely free of charge. Their commitment to democratizing access to cutting-edge data practices has been instrumental in enabling projects like this one to come to life.