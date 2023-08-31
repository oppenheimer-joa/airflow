from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.models.variable import Variable
import requests
import pendulum

local_tz = pendulum.timezone("Asia/Seoul")

SERVER_API = Variable.get("SERVER_API")
category = 'movieImages'
date = "{{execution_date.add(days=364, hours=9).strftime('%Y-%m-%d')}}"

default_args = {
    'owner': 'sms/v0.7.0',
    'depends_on_past': True,
    'start_date': datetime(1999, 7, 3, tzinfo=local_tz),
    "provide_context":True,
}

dag = DAG(
    dag_id='get_TMDB_images',
    default_args=default_args,
    schedule="0 3 * * 5", ## 990703+182일~ 매주 금요일 AM 03:00 실행
    tags= ['수집','TMDB','images'],
    max_active_runs= 1
)


# 데이터 수집 API 호출
def get_api_data(**context):
    api_url_get_data = f"http://{SERVER_API}/tmdb/movie-images?date={date}"
    response = requests.get(api_url_get_data)
    # 오류 처리
    response.raise_for_status()
    # JSON 응답 파싱
    data = response.json()
    print(data)
    
    db_counts = data[0]
    
    # context를 사용하여 XCom에 값을 저장합니다.
    ti = context['ti']
    ti.xcom_push(key='db_counts', value=db_counts)
    
    return db_counts


# # 정합성  체크
def check_logic(category, date, **context):
    # XCom에서 값을 가져옵니다.
    ti = context['ti']
    xcom = ti.xcom_pull(task_ids='Get.TMDB_Images_Data', key='db_counts')
    print(xcom)
    api_url_check_data = f"http://{SERVER_API}/check/tmdb?xcom={xcom}&category={category}&date={date}"
    response = requests.get(api_url_check_data)
    # 오류 처리
    response.raise_for_status()
    # JSON 응답 파싱
    data = response.text
    if data == '1':
        return 'ERROR'
    else:
        return 'DONE'

#target_date format yyyy-mm-dd
def erase_loaded_data(target_date):
    import subprocess

    base_url = f"http://{SERVER_API}/cleansing/images"
    curl_url = f"{base_url}?target_date={target_date}"
    command = ["curl", curl_url]
    try:
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
        print("Curl command output:", result.stdout)
    except subprocess.CalledProcessError as e:
        print("err:", e.stderr)

def blob_data(date_gte, base_url):
	import subprocess
	curl_url = f"{base_url}?date={date_gte}"
	command = ["curl", curl_url]
	subprocess.run(command)

start = EmptyOperator(task_id = 'Start.task', dag = dag)
get_data = PythonOperator(task_id = "Get.TMDB_Images_Data", python_callable=get_api_data, dag = dag)

branching = BranchPythonOperator(task_id='Check.Integrity',python_callable=check_logic, op_args=[category, date], dag=dag)

push_data = PythonOperator(task_id = "Push.TMDB_Images_Data", python_callable=blob_data, op_args=[date, f'http://{SERVER_API}/blob/tmdb/images'], dag = dag)
cleansing_data = PythonOperator(task_id = 'delete.TMDB.movieImages.datas',python_callable=erase_loaded_data, op_args=['{{next_execution_date.strftime("%Y-%m-%d")}}'], dag = dag)

error = EmptyOperator(task_id = 'ERROR', dag = dag)
done = EmptyOperator(task_id = 'DONE', dag = dag)

finish = EmptyOperator(task_id = 'Finish.task', dag = dag)


start >> get_data >> branching
branching >> error
branching >> done >> push_data >> cleansing_data >> finish