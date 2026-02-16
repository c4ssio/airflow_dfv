FROM apache/airflow:3.1.5

USER root
USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Bake DAGs and plugins into the image for ECS Fargate deployment.
# In local dev, docker-compose volume mounts shadow these paths.
COPY dags/ /opt/airflow/dags/
COPY plugins/ /opt/airflow/plugins/