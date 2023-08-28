FROM apache/airflow:2.6.1-python3.8
RUN pip install --no-cache-dir -r requirements.txt
