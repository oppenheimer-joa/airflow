from airflow import DAG
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime
import mysql.connector, pendulum, sys

# 변수 정의
KST = pendulum.timezone("Asia/Seoul")
exe_date = "{{ execution_date.strftime('%Y%m%d') }}"
SERVER_API = Variable.get("SERVER_API")

default_args ={
    'owner' : 'sms/v0.7.0',
    'depends_on_past' : True,
    'start_date' : datetime(2023, 8, 1, tzinfo=KST)
}

dag = DAG('load_kobis_boxOffice_API-01',
	  default_args = default_args,
      max_active_runs = 1,
      tags =['수집','박스오피스', 'KOBIS'],
      schedule_interval ='0 0 * * *')   ## 매일 00:00 에 실행

def movie_location_code_from_db(**context):
	MYSQL_HOST = Variable.get("DB_HOST")
	MYSQL_PORT = Variable.get("DB_PORT")
	MYSQL_USER = Variable.get("DB_USER")
	MYSQL_PWD = Variable.get("DB_PWD")
	MYSQL_DB = Variable.get("DB_DB")

	conn = mysql.connector.connect(host=MYSQL_HOST, password=MYSQL_PWD, port=MYSQL_PORT, user=MYSQL_USER, database=MYSQL_DB)
	cursor = conn.cursor()

	query = "select * from movie_location"
	cursor.execute(query)
	location_raw_data = cursor.fetchall()
	print(location_raw_data)

	area_code_list = []
	for i in location_raw_data:
		area_code_list.append(i[0])

	print(area_code_list)
	return area_code_list


def send_load_curl(date,**context):
	import subprocess
	print(date)
	base_url = f"http://{SERVER_API}/kobis/daily-boxoffice"
	print(base_url)
	area_code_list = context['task_instance'].xcom_pull(task_ids='get_movie_location_code')
	for i in area_code_list:
		curl_url = f"{base_url}?now_date='{date}'&area_code='{i}'"
		command = f"curl '{curl_url}'"

		try:
			output = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
			print("Curl command output:", output.stdout)

		except subprocess.CalledProcessError as e:
			print("err:", e.stderr)

def send_check_curl():
	import subprocess
	base_url = f"http://{SERVER_API}/check/boxoffice"
	command = f"curl {base_url}"
	output = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
	if output == "1":
		sys.exit(1)

# 데이터 s3에 넣기
def blob_data(date, base_url):
	import subprocess
	curl_url = f"{base_url}?date={date}"
	command = ["curl", curl_url]
	subprocess.run(command)
  
def deleted_loaded_data(date):
	import subprocess
	base_url = f"http://{SERVER_API}/cleansing/boxoffice"
	
	# ex_cmd = http://{SERVER_API}/cleansing/kobis?now_date=2023-01-01
	curl_url = f"{base_url}?now_date='{date}'"
	command = f"curl '{curl_url}'"

	try:
		output = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
		print("Curl command output:", output.stdout)

	except subprocess.CalledProcessError as e:
		print("err:", e.stderr)




start = EmptyOperator(
    task_id = 'start_task',
    dag = dag)

get_movie_location_code_from_db = PythonOperator(
	task_id = 'get_movie_location_code',
	python_callable=movie_location_code_from_db,
	# provide_context=True,
	dag=dag
	)

load_daily_BoxOffice = PythonOperator(
	task_id = 'load_daily_BoxOffice',
	python_callable=send_load_curl,
	op_kwargs={"date": exe_date},
	# provide_context=True,
	dag=dag
	)

check_files = PythonOperator(
	task_id = 'check_BoxOffice_files',
	python_callable=send_check_curl,
	dag=dag
	)

# blob Operator 추가
push_data = PythonOperator(
	task_id = "Push.Boxoffice_Data",
    python_callable=blob_data,
    op_args=[exe_date, f'http://{SERVER_API}/blob/boxoffice'],
    dag = dag)

# cleansing Operator 추가
cleansing_data = PythonOperator(
	task_id = 'delete.KOBIS.boxOffice.datas',
	python_callable = deleted_loaded_data,
	op_kwargs={"date": exe_date},
	dag = dag
	)

finish = EmptyOperator(
    task_id = 'finish',
    trigger_rule = 'none_failed',
    dag = dag)

start >> get_movie_location_code_from_db >> load_daily_BoxOffice >> check_files
check_files >> push_data >> cleansing_data >> finish

