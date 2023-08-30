from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.variable import Variable
import pendulum, requests, json, os
from datetime import datetime

KST = pendulum.timezone('Asia/Seoul')

default_args = {
	'owner': 'v0.3.0/yoda/dev',
	'depends_on_past' : False,
	'start_date': datetime(1930,1,1, tzinfo=KST)}

dag = DAG('get_academy_datas', default_args = default_args, max_active_runs= 1, tags=['수집','IMDB', 'academy'] , schedule_interval= '20 0 10 5 *')

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
	op_args=['academy', '{{next_execution_date.strftime("%Y")}}','192.168.90.128:4551/imdb/award'],
	dag = dag
	)

check_datas = PythonOperator(
	task_id = 'check_academy_datas',
	python_callable = send_req,
	op_args =['academy','{{next_execution_date.strftime("%Y")}}','192.168.90.128:4551/check/imdb'])

end_task = EmptyOperator(
	task_id = 'finish_academy_data_task',
	dag = dag)

start_task >> load_datas >> check_datas >> end_task
