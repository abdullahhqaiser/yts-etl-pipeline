FROM apache/airflow:2.3.3
RUN pip install --no-cache-dir apache-airflow-providers-microsoft-mssql
RUN pip install --no-cache-dir apache-airflow-providers-jdbc
