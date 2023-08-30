from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.variable import Variable
import pendulum
import requests

local_tz = pendulum.timezone("Asia/Seoul")
SERVER_API = Variable.get("SERVER_API")
EVENT_IDS = {'academy' : 'ev0000003',
             'cannes': 'ev0000147',
             'venice' : 'ev0000681',
             'busan': 'ev0004044'}

default_args = {
  'start_date': datetime(1961, 11, 1, tzinfo=local_tz),
  'owner': 'SMS'
}

dag = DAG(
  dag_id = 'IMDB_BUSAN_DATA',
  description = 'IMDB data pipeline for Busan',
  tags = ['IMDB','Awards','Busan'],
  schedule_interval = '* * 1 11 *',   ### 스케줄 정의 필요 # 임시 매년 11월 1일 
  user_defined_macros={'local_dt': lambda execution_date: execution_date.in_timezone(local_tz).strftime("%Y-%m-%d %H:%M:%S")},
  default_args=default_args
)


# 함수 정의
## 수상작 정보 크롤링 API 호출
def imdb_data_load(event, year) :
    api_url = f"http://{SERVER_API}/imdb/award?event={event}&year={year}"
    response = requests.get(api_url).json()

    print(response)

#정합성  체크
def check_logic(event, year) :
    api_url = f"http://{SERVER_API}/check/imdb?event={event}&year={year}"
    response = requests.get(api_url).json()

    if response == '1' :
        return "ERROR"
    
    else:
        return "DONE"
    

# Operator 정의
start = EmptyOperator(task_id = 'Stark.task', dag = dag)
finish = EmptyOperator(task_id = 'Finish.task', trigger_rule='one_success', dag = dag)

load_tasks = PythonOperator(task_id="Save.Imdb_busan",
                            python_callable=imdb_data_load,
                            op_kwargs={"event": "busan", "year": "{{next_execution_date.in_timezone('Asia/Seoul').strftime('%Y')}}" },
                            dag=dag)

branching = BranchPythonOperator(task_id='Check.logic',
                                 python_callable=check_logic,
                                 op_kwargs={"event": "busan", "year": "{{next_execution_date.in_timezone('Asia/Seoul').strftime('%Y')}}" },
                                 dag=dag)

error = EmptyOperator(task_id = 'ERROR', dag = dag)
done = EmptyOperator(task_id = 'DONE', dag = dag)
    
# Operator 배치
start >> load_tasks >> branching
branching >> error >> finish 
branching >> done >> finish 