from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.variable import Variable
import pendulum
import requests

local_tz = pendulum.timezone("Asia/Seoul")
SERVER_API = Variable.get("SERVER_API")
DAGS_OWNER = Variable.get('DAGS_OWNER')

default_args = {
  'owner': DAGS_OWNER,
  'depends_on_past': True,
  'start_date': datetime(1961, 7, 1, tzinfo=local_tz)
}

dag = DAG(
  dag_id = 'load_imdb_cannes_API-01',
  description = 'IMDB data pipeline for cannes',
  tags = ['수집','IMDB','cannes'],
  schedule_interval = '0 11 1 7 *',   ## 매년 7월 1일 AM 11:00 실행
  max_active_runs= 1,
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
def check_logic(event, year):
    api_url = f"http://{SERVER_API}/check/imdb?event={event}&year={year}"
    response = requests.get(api_url).json()

    if response == '1' :
        return "ERROR"
    
    else:
        return "DONE"
      
# Blob
def push_datas(event, year):
	import subprocess
	curl_url = f"http://{SERVER_API}/blob/imdb?event={event}&year={year}"
	command = ["curl", curl_url]
	subprocess.run(command)

def erase_datas(event, year):
    api_url = f"http://{SERVER_API}/cleansing/imdb?event={event}&year={year}"
    response = requests.get(api_url).json()
    print(response)
    

# Operator 정의
start = EmptyOperator(task_id = 'start_cannes_data_task', dag = dag)

load_tasks = PythonOperator(task_id="get_cannes_datas",
                            python_callable=imdb_data_load,
                            op_kwargs={"event": "cannes", "year": "{{next_execution_date.in_timezone('Asia/Seoul').strftime('%Y')}}" },
                            dag=dag)

branching = BranchPythonOperator(task_id='check_cannes_datas',
                                 python_callable=check_logic,
                                 op_kwargs={"event": "cannes", "year": "{{next_execution_date.in_timezone('Asia/Seoul').strftime('%Y')}}" },
                                 dag=dag)

error = EmptyOperator(task_id = 'ERROR', dag = dag)
done = EmptyOperator(task_id = 'DONE', dag = dag)

push_data = PythonOperator(task_id = 'blob_cannes_datas',
                           python_callable = push_datas,
                           op_kwargs={"event": "cannes", "year": "{{next_execution_date.in_timezone('Asia/Seoul').strftime('%Y')}}"},
                           dag = dag)

cleansing_data = PythonOperator(task_id = "delete_cannes_datas",
                                python_callable=erase_datas,
                                op_kwargs={"event": "cannes", "year": "{{next_execution_date.in_timezone('Asia/Seoul').strftime('%Y')}}"},
                                dag = dag)

finish = EmptyOperator(task_id = 'finish_caanes_data_task', trigger_rule='one_success', dag = dag)

# Operator 배치
start >> load_tasks >> branching
branching >> [error, done]
done >> push_data >> cleansing_data >> finish