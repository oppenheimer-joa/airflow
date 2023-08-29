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
EVENT_IDS = {'academy' : 'ev0000003',
             'cannes': 'ev0000147',
             'venice' : 'ev0000681',
             'busan': 'ev0004044'}

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
def imdb_data_load(event, year) :
    api_url = f"http://{SERVER_API}/imdb/award?event={event}&year={year}"
    response = requests.get(api_url).json()
    print("응답 수행 완료")
    return response
    

## 정합성 체크 함수
def imdb_file_check(event, year):  ### params : event,year
    file_path = f"/api/datas/IMDb/imdb_{event}_{year}.json"
    if os.path.isfile(file_path):
        size = os.path.getsize(file_path)
        if size >= 8000:
            return f"{file_path} size is {size} Byte"
    else:
        return "1"


# Operator 정의

start = EmptyOperator(task_id = 'Start.task', dag = dag)
finish_load = EmptyOperator(task_id = 'Load.Done', dag = dag)
finish = EmptyOperator(task_id = 'Finish.task', dag = dag)
# error = EmptyOperator(task_id = 'ERROR', dag = dag)

load_tasks = list()
for event,code in EVENT_IDS.items() :
    load_task = PythonOperator(task_id=f"Get.IMDB_{event}_data",
                          python_callable=imdb_data_load,
                          op_kwargs={
                              "event" : "{{code}}",
                              "year": "{{execution_date.in_timezone('Asia/Seoul').strftime('%Y')}}"
                              },
                          dag=dag)
    load_tasks.append(load_task)


check_tasks = list()
for event,code in EVENT_IDS.items() :
    check_task = PythonOperator(task_id=f"Check.IMDB_{event}_data",
                          python_callable=imdb_file_check,
                          op_kwargs={
                              "event" : "{{code}}",
                              "year": "{{execution_date.in_timezone('Asia/Seoul').strftime('%Y')}}"
                              },
                          dag=dag)
    check_tasks.append(check_task)
    
# Operator 배치
start >> load_tasks >> finish_load >> check_tasks >> finish ## 마지막 분기 수정해주세용