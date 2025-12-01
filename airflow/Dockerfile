FROM apache/airflow:2.10.5

USER airflow

ARG AIRFLOW_VERSION=2.10.5

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt