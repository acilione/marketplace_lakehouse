FROM apache/airflow:3.3.1-python3.12

ARG AIRFLOW_VERSION=3.3.1
ARG PYTHON_VERSION=3.12
USER airflow
COPY pyproject.toml README.md LICENSE /opt/marketplace/
COPY requirements/ /opt/marketplace/requirements/
COPY src/ /opt/marketplace/src/
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" && \
    pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      "apache-airflow-providers-apache-spark==6.3.2" \
      "pyspark==4.1.3" && \
    pip install --no-cache-dir \
      --constraint /opt/marketplace/requirements/runtime.lock /opt/marketplace

ENV PYTHONPATH=/opt/marketplace/src
