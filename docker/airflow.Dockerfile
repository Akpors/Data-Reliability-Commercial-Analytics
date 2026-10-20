ARG AIRFLOW_VERSION=3.3.2
FROM apache/airflow:${AIRFLOW_VERSION}-python3.12

# Re-declared: ARGs before FROM are not visible after it.
ARG AIRFLOW_VERSION

COPY requirements/airflow.txt requirements/dbt.txt /tmp/requirements/

# Project requirements are installed against Airflow's constraints so they cannot
# upgrade or downgrade anything Airflow itself depends on.
RUN pip install --no-cache-dir -r /tmp/requirements/airflow.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.12.txt"

# dbt lives in its own venv, isolated from Airflow's constraints.
USER root
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements/dbt.txt \
    && chown -R airflow:0 /opt/dbt-venv
USER airflow

ENV DBT_BIN=/opt/dbt-venv/bin/dbt
