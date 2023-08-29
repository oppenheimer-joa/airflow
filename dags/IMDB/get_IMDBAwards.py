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
  tags = ['airflow','IMDB'],
  schedule_interval = '* * 1 * *',   ### 스케줄 정의 필요
  user_defined_macros={'local_dt': lambda execution_date: execution_date.in_timezone(local_tz).strftime("%Y-%m-%d %H:%M:%S")},
  default_args=default_args
)


# 함수 정의
## 수상작 정보 크롤링 함수
def imdb_data_load(event, start_year) :
    api_url = f"http://{SERVER_API}/imdb/award?event={event}&year={start_year}"
    response = requests.get(api_url).json()
    print("응답 수행 완료")
    return response
    

## 정합성 체크 함수
def imdb_file_check(event, start_year):  ### params : event,start_year
    file_path = f"/api/datas/IMDb/imdb_{event}_{start_year}.json"
    if os.path.isfile(file_path):
        size = os.path.getsize(file_path)
        if size >= 8000:
            return f"{file_path} size is {size} Byte"
    else:
        return "1"


# Operator 정의

start = EmptyOperator(task_id = 'Start.task', dag = dag)
error = EmptyOperator(task_id = 'ERROR', dag = dag)
finish = EmptyOperator(task_id = 'Finish.task', dag = dag)

get_IMDBAwards = PythonOperator(task_id="Get.IMDB_Awards",
                            python_callable=imdb_data_load,
                            op_kwargs={
                                "event" : "",
                                "start_date": "{{execution_date.in_timezone('Asia/Seoul').strftime('%Y')}}"
                                },
                            dag=dag)

check_IMDBAwards = PythonOperator(task_id="Check.IMDB_Awards",
                                  python_callable=imdb_file_check,
                                  op_kwargs={
                                  "event" : "",
                                  "start_date": "{{execution_date.in_timezone('Asia/Seoul').strftime('%Y')}}"
                                },
                                  provide_context=True,
                                  dag=dag)
# Operator 배치
start >> get_IMDBAwards >> check_IMDBAwards >> finish