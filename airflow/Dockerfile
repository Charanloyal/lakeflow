# Airflow 2.10 (LocalExecutor via `standalone`) + the Trino client + the LakeFlow core library used by the DAGs.
FROM apache/airflow:3.3.2-python3.11
COPY airflow/requirements.txt /tmp/lakeflow-requirements.txt
# Pinning apache-airflow in the same install stops pip from silently up/downgrading Airflow itself.
RUN pip install --no-cache-dir "apache-airflow==2.10.5" -r /tmp/lakeflow-requirements.txt
COPY --chown=airflow:root libs/lakeflow_core /opt/lakeflow/libs/lakeflow_core
ENV PYTHONPATH=/opt/lakeflow/libs
