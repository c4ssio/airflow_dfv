FROM apache/airflow:3.1.5

USER root
# (optional) system deps could go here
USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.tx