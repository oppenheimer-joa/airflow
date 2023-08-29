from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.models.variable import Variable
from datetime import datetime
import pendulum, mysql.connector

KST = pendulum.timezone('Asia/Seoul')

default_args ={
	'owner': 'v0.1.0/sms',
	'depends_on_past' : False,
	'start_date': datetime(2023,1,1, tzinfo=KST)}

dag = DAG('get_spotify_datas', default_args = default_args, max_active_runs= 1, tags=['수집','spotify'] , schedule_interval= '0 10 * * 1')

params_date = "{{execution_date.strftime('%Y-%m-%d')}}"

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

def extract_moiveCode_data():

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

	cursor = conn.cursor()
	#print(params_date)
	query = f"select * from movie_all where date_gte = '{{execution_date}}'"
	
	cursor.execute(query)
	raw_datas = cursor.fetchall()
	for i in range(len(raw_datas)):
		movieCode_list.append(data[i][0]) 
	conn.close()
	print(movieCode_list)
	return movieCode_list
	

def send_curl_requests(movieCode_list):
	import subprocess
	base_url = "http://192.168.90.128:4551/spotify/movie-ost"
	for row in data:
		movie_code = row[1]
		curl_url = f"{base_url}?movieCode={movie_code}"
		command = ["curl", curl_url]

		try:
			result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
			print("Curl command output:", result.stdout)
		except subprocess.CalledProcessError as e:
			print("err:", e.stderr)

extract_task = PythonOperator(
	task_id='extract_movie_codes',
	python_callable=extract_moiveCode_data,
	provide_context=True,
	dag=dag
)

send_task = PythonOperator(
	task_id='send_curl_requests',
	python_callable=send_curl_requests,
	op_args=[],
	provide_context=True,
	dag=dag
)

extract_task >> send_task



