from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.models.variable import Variable
from datetime import datetime
import pendulum, mysql.connector

KST = pendulum.timezone('Asia/Seoul')
SERVER_API = Variable.get("SERVER_API")

default_args ={
	'owner': 'sms/v0.7.0',
	'depends_on_past' : True,
	'start_date': datetime(2023,1,1, tzinfo=KST)}

dag = DAG(dag_id='load_spotify_OST_API-01',
	  default_args = default_args,
      max_active_runs= 1,
      tags=['수집','spotify','영화OST'] ,
      schedule_interval= '0 2 * * 5')  ## 매주 금요일 AM 02:00 실행

def gen_curl_operator(name: str, url : str, trigger_opt: str):
    #curl 싸개

    curl_cmd = """
        curl link
    """

    curl_cmd = curl_cmd.replace("link", url)

    bash_task = BashOperator(
        task_id=name,
        bash_command=curl_cmd,
        trigger_rule = trigger_opt,
        dag=dag
    )

    return bash_task

def extract_moiveCode_data(execution_date):

	DB_HOST = Variable.get("DB_HOST")
	DB_USER = Variable.get("DB_USER")
	DB_PORT = Variable.get("DB_PORT")
	DB_PWD = Variable.get("DB_PWD")
	DB_DB = Variable.get("DB_DB")


	movieCode_list = []

	conn = mysql.connector.connect(
		host=DB_HOST,
		user=DB_USER,
		port=DB_PORT,
		password=DB_PWD,
		database=DB_DB
	)
	exe_dt = execution_date.in_timezone('Asia/Seoul').strftime('%Y-%m-%d')
	print(exe_dt)
	cursor = conn.cursor()
	query = f"select * from movie where date_gte = '{exe_dt}'"
	print(query)
	cursor.execute(query)
	raw_datas = cursor.fetchall()
	for i in range(len(raw_datas)):
		movieCode_list.append(raw_datas[i][0]) 
	conn.close()
	return movieCode_list
	
def send_curl_requests(movieCode_list):
	import subprocess
	base_url = f"http://{SERVER_API}/spotify/movie-ost"

	for code in movieCode_list:
		curl_url = f"{base_url}?movieCode={code}"
		command = ["curl", curl_url]

		try:
			result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
			print("Curl command output:", result.stdout)
		except subprocess.CalledProcessError as e:
			print("err:", e.stderr)

# Blob
def blob_data(base_url):
	import subprocess
	curl_url = f"{base_url}"
	command = ["curl", curl_url]
	subprocess.run(command)
	
def erase_loaded_data(target_date):
	import subprocess
	base_url = f"http://{SERVER_API}/cleansing/spotify"
	curl_url = f"{base_url}?target_date={target_date}"
	command = ["curl", curl_url]
	try:
		result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
		print("Curl command output:", result.stdout)
	except subprocess.CalledProcessError as e:
		print("err:", e.stderr)


start_task = EmptyOperator(
	task_id = 'start_spotify_datas_task',
	dag = dag)

extract_task = PythonOperator(
	task_id='extract_movie_codes',
	python_callable=extract_moiveCode_data,
	provide_context=True,
	dag=dag
)

send_task = PythonOperator(
	task_id='send_curl_requests',
	python_callable=send_curl_requests,
	op_args=[extract_task.output],
	provide_context=True,
	dag=dag
)

push_data = PythonOperator(
	task_id = "Push.Spotify_Data",
	python_callable=blob_data,
	op_args=[f'http://{SERVER_API}/blob/spotify'],
	dag = dag)

cleansing_data = PythonOperator(
	task_id = 'delete.spotify.OST.datas',
	python_callable=erase_loaded_data,
	dag = dag)

end_task = EmptyOperator(
	task_id = 'finish_spotify_datas_task',
	dag = dag)

start_task >> extract_task >> send_task >> push_data >> cleansing_data >>  end_task



