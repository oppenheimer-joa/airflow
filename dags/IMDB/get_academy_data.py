from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.variable import Variable
import pendulum, requests, json, os

default_args = {
	'owner': 'v0.3.0/yoda/dev',
	'depends_on_past' : False,
	'start_date': datetime(1930,5,1, tzinfo=KST)}

dag = DAG('get_academy_datas', default_args = default_args, max_active_runs= 1, tags=['수집','IMDB', 'academy'] , schedule_interval= '20 0 10 5 *')

def gen_curl_operator(name: str, url : str, trigger_opt: str):
    #curl 싸개

    curl_cmd = """
        curl "link"
    """

    curl_cmd = curl_cmd.replace("link", url)

    bash_task = BashOperator(
        task_id=name,
        bash_command=curl_cmd,
        trigger_rule = trigger_opt,
        dag=dag
    )

    return bash_task

start_task = EmptyOperator(
	task_id = 'start_academy_data_task',
	dag = dag)

get_data = BashOperator(
	task_id = 'get_academy_datas',
	bash_command = "curl '192.168.90.128:4551/imdb/award?event=academy&year={{next_execution_date}}'",
	dag = dag)


end_task = EmptyOperator(
	task_id = 'finish_academy_data_task',
	dag = dag)

start_task >> get_data >> end_task
