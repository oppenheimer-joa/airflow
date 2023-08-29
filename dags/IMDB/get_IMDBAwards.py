from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.variable import Variable
import pendulum
import requests, json, os

local_tz = pendulum.timezone("Asia/Seoul")
SERVER_API = Variable.get("SERVER_API")

default_args = {
  'start_date': datetime(2023, 8, 28, tzinfo=local_tz),
  'owner': 'SMS',
  'retries': 0,
  'retry_delay': timedelta(minutes=1)
}

dag = DAG(
  dag_id = 'IMDB_DATA',
  description = 'IMDB data pipeline',
  tags = ['airflow'],
  schedule_interval = '* * 1 * *',   ### 스케줄 정의 필요
  user_defined_macros={'local_dt': lambda execution_date: execution_date.in_timezone(local_tz).strftime("%Y-%m-%d %H:%M:%S")},
  default_args=default_args
)

