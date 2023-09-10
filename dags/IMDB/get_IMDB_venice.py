from airflow import DAG
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.models.variable import Variable
from datetime import datetime
import mysql.connector, pendulum

# 변수 정의
KST = pendulum.timezone("Asia/Seoul")
year_str = "{{ next_execution_date.strftime('%Y') }}"
SERVER_API = Variable.get("SERVER_API")
DAGS_OWNER = Variable.get('DAGS_OWNER')

default_args ={
    'owner' : DAGS_OWNER,
    'depends_on_past' : False,
    'start_date' : datetime(1959, 10, 1, tzinfo=KST)
}

dag = DAG('load_imdb_venice_API-01',
          default_args = default_args,
          max_active_runs = 1,
          tags =['수집','IMDB','venice'],
          schedule_interval = '0 11 1 10 *',   ## 매년 10월 1일 AM 11:00 실행
          )

def send_load_curl(event, year):
    import subprocess, sys
    base_url = f"http://{SERVER_API}/imdb/award"
    curl_url = f"{base_url}?event='{event}'&year='{year}'"
    command = f"curl '{curl_url}'"
    output = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    print(output)

def send_check_curl(event, year):
    import subprocess, sys
    base_url = f"http://{SERVER_API}/check/imdb"
    curl_url = f"{base_url}?event='{event}'&year='{year}'"
    command = f"curl '{curl_url}'"
    output = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if output == "1":
        sys.exit(1)

def blob_data(event, year):
	import subprocess
	curl_url = f"http://{SERVER_API}/blob/imdb?event={event}&year={year}"
	command = ["curl", curl_url]
	subprocess.run(command)
  
def erase_loaded_data(event, year):
    import subprocess, sys
    base_url = f"http://{SERVER_API}/cleansing/imdb"
    curl_url = f"{base_url}?event='{event}'&year='{year}'"
    command = f"curl '{curl_url}'"
    output = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    print(output)
   
start = EmptyOperator(
    task_id = 'start_venice_data_task',
    dag = dag)

load_data = PythonOperator(
    task_id = 'get_venice_datas',
    python_callable=send_load_curl,
    op_kwargs={"event":"venice",
               "year":year_str},
    dag=dag)

check_data = PythonOperator(
    task_id = 'check_venice_datas',
    python_callable=send_check_curl,
    op_kwargs={"event":"venice",
               "year":year_str},
    dag=dag)

push_datas = PythonOperator(
	task_id = 'blob_venice_datas',
	python_callable = blob_data,
	op_kwargs={"event":"venice",
               "year":year_str},
	dag = dag)

cleansing_data = PythonOperator(
    task_id = 'delete_venice_datas',
    python_callable=erase_loaded_data,
    op_kwargs={"event":"venice",
               "year":year_str},
    dag=dag)

finish = EmptyOperator(
    task_id = 'finish_venice_data_task',
    trigger_rule = 'none_failed',
    dag = dag)
# Blob


start >> load_data >> check_data >> push_datas >> cleansing_data >> finish