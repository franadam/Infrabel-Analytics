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