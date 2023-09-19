FROM apache/airflow:2.6.1-python3.8

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# 나머지 Dockerfile 내용 추가
