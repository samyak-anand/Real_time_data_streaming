#!/bin/bash
set -e

# AIRFLOW_UID=5000
mkdir -p /opt/airflow/logs /opt/airflow/dags /opt/airflow/plugins
# chown -R "${AIRFLOW_UID}:0" /opt/airflow/{logs,dags,plugins}

if [ -e "/opt/airflow/requirements.txt" ]; then
  $(command python) pip install --upgrade pip
  $(command -v pip) install -r requirements.txt
fi

if [ ! -f "/opt/airflow/airflow.db" ]; then
  airflow db init && \
  airflow users create \
    --username airflow \
    --firstname airflow \
    --lastname airflow \
    --role Admin \
    --email admin@example.com \
    --password airflow
fi

$(command -v airflow) db upgrade

exec airflow webserver
