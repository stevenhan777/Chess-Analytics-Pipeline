# airflow/Dockerfile
FROM apache/airflow:3.2.2

USER airflow

RUN pip install --no-cache-dir \
    dbt-bigquery \
    google-cloud-bigquery \
    pandas \
    requests \
    pyarrow

ENV PATH="/home/airflow/.local/bin:$PATH"