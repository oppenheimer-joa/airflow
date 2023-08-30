from airflow import DAG
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import mysql.connector, pendulum

# 변수 정의
KST = pendulum.timezone("Asia/Seoul")
year_str = "{{ next_execution_date.strftime('%Y') }}"

default_args ={
    'owner' : 'v0.3.0/woorek',
    'depends_on_past' : False,
    'start_date' : datetime(1959, 10, 1, tzinfo=KST)
}

dag = DAG('IMDB_VENICE', default_args = default_args, max_active_runs = 1, tags =['수집','베니스 영화제'], schedule_interval ='@yearly')

def send_load_curl(event, year):
    import subprocess, sys
    base_url = "192.168.90.128:4551/imdb/award"
    curl_url = f"{base_url}?event='{event}'&year='{year}'"
    command = f"curl '{curl_url}'"
    output = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    print(output)

def send_check_curl(event, year):
    import subprocess, sys
    base_url = f"192.168.90.128:4551/check/imdb/"
    curl_url = f"{base_url}?event='{event}'&year='{year}'"
    command = f"curl '{curl_url}'"
    output = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if output == "1":
        sys.exit(1)

start = EmptyOperator(
    task_id = 'start_task',
    dag = dag)

load_data = PythonOperator(
    task_id = 'get_IMDB_venice',
    python_callable=send_load_curl,
    op_kwargs={"event":"venice",
               "year":year_str},
    dag=dag)

check_data = PythonOperator(
    task_id = 'check_IMDB_venice',
    python_callable=send_check_curl,
    op_kwargs={"event":"venice",
               "year":year_str},
    dag=dag)

finish = EmptyOperator(
    task_id = 'finish',
    trigger_rule = 'none_failed',
    dag = dag)

start >> load_data >> check_data >> finish