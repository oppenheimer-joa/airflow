from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.variable import Variable
import pendulum, requests, json, os
from datetime import datetime

KST = pendulum.timezone('Asia/Seoul')
SERVER_API = Variable.get("SERVER_API")
DAGS_OWNER = Variable.get('DAGS_OWNER')

default_args = {
	'owner': DAGS_OWNER,
	'depends_on_past': True,
	'start_date':datetime(1930, 1, 1, tzinfo=KST)}

dag = DAG('load_imdb_academy_API-01',
	  default_args = default_args,
      max_active_runs= 1,
      tags=['수집','IMDB', 'academy'],
      schedule_interval= '0 11 1 5 *')   ## 매년 5월 1일 AM 11:00 실행

def send_req(event, exe_year, base_url):
	import subprocess
	curl_url = f"{base_url}?event={event}&year={exe_year}"
	command = ["curl", curl_url]
	subprocess.run(command)


start_task = EmptyOperator(
	task_id = 'start_academy_data_task',
	dag = dag)

load_datas = PythonOperator(
	task_id = 'get_academy_datas',
	python_callable = send_req,
	op_args=['academy', '{{next_execution_date.strftime("%Y")}}',f'http://{SERVER_API}/imdb/award'],
	dag = dag
	)

check_datas = PythonOperator(
	task_id = 'check_academy_datas',
	python_callable = send_req,
	op_args =['academy','{{next_execution_date.strftime("%Y")}}',f'http://{SERVER_API}/check/imdb'],
	dag = dag)

push_data = PythonOperator(
	task_id = 'push_academy_datas',
	python_callable = send_req,
	op_args =['academy','{{next_execution_date.strftime("%Y")}}',f'http://{SERVER_API}/blob/imdb'],
	dag = dag)

cleansing_data = PythonOperator(
	task_id = 'delete_acadmey_datas',
	python_callable= send_req,
	op_args=['academy', '{{next_execution_date.strftime("%Y")}}', f'http://{SERVER_API}/cleansing/imdb'],
	dag = dag)

end_task = EmptyOperator(
	task_id = 'finish_academy_data_task',
	dag = dag)

start_task >> load_datas >> check_datas >> push_data >> cleansing_data >> end_task
