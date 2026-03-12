FROM apache/airflow:2.9.3-python3.11
USER root

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir dbt-core==1.7.0 dbt-clickhouse==1.7.6
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         nano \
	 iputils-ping \
	 git
COPY ./.dbt /home/airflow/.dbt
USER airflow