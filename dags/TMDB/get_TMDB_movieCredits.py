from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.models.variable import Variable
import requests
import pendulum

local_tz = pendulum.timezone("Asia/Seoul")
DAGS_OWNER = Variable.get('DAGS_OWNER')
SERVER_API = Variable.get("SERVER_API")
category = 'movieCredits'

default_args = {
    'owner': DAGS_OWNER,
    'depends_on_past': True,
    'start_date': datetime(1999, 7, 3, tzinfo=local_tz),
    "provide_context":True,
}

dag = DAG(
    'load_tmdb_credits_API-01',
    default_args=default_args,
    schedule="0 3 * * 5", ## 990703~ 매주 금요일 AM 03:00 실행
    tags = ['수집','TMDB','credits'],
    max_active_runs= 1
)


# 데이터 수집 API 호출
def get_api_data(date, **context):
    api_url = f"http://{SERVER_API}/tmdb/movie-credits?date={date}"
    response = requests.get(api_url)
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

# 정합성  체크
def check_logic(category, date, **context):
    # XCom에서 값을 가져옵니다.
    ti = context['ti']
    xcom = ti.xcom_pull(task_ids='get_TMDB.Credits_datas', key='db_counts')
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
    
# 데이터 s3에 넣기
#http://192.168.90.128:4553/blob/tmdb?category=movieCredits&date=2000-01-07
def blob_data(date_gte, base_url):
    import subprocess
    curl_url = f"{base_url}?category={category}&date={date_gte}"  
    print(curl_url, date_gte)  
    command = ["curl", curl_url]
    subprocess.run(command)

# target_date format yyyy-mm-dd
def erase_loaded_data(target_date):
    import subprocess

    base_url = f"http://{SERVER_API}/cleansing/tmdb"
    curl_url = f"{base_url}?category={category}&date={target_date}"
    print(curl_url)
    command = ["curl", curl_url]

    try:
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
        print("Curl command output:", result.stdout)
    except subprocess.CalledProcessError as e:
        print("err:", e.stderr)

start = EmptyOperator(task_id = 'start_TMDB.Credits_task', dag = dag)
get_data = PythonOperator(task_id = "get_TMDB.Credits_datas", python_callable=get_api_data, op_args=["{{execution_date.add(days=182, hours=9).strftime('%Y-%m-%d')}}"], dag = dag)

branching = BranchPythonOperator(task_id='check_integrity',python_callable=check_logic, op_args=[category,"{{execution_date.add(days=182, hours=9).strftime('%Y-%m-%d')}}"], dag=dag)

error = EmptyOperator(task_id = 'ERROR', dag = dag)
done = EmptyOperator(task_id = 'DONE', dag = dag)

push_data = PythonOperator(task_id = "push_TMDB.Credits_datas", python_callable=blob_data, op_args=["{{execution_date.add(days=182, hours=9).strftime('%Y-%m-%d')}}", f'http://{SERVER_API}/blob/tmdb'], dag = dag)
cleansing_data = PythonOperator(
    task_id = 'delete_TMDB.Credits_datas',
    python_callable=erase_loaded_data,
    op_args=["{{execution_date.add(days=182, hours=9).strftime('%Y-%m-%d')}}"],
    dag = dag)

finish = EmptyOperator(task_id = 'finish_TMDB.Credits_task', dag = dag)

# 배치
start >> get_data >> branching
branching >> error
branching >> done >> push_data >> cleansing_data >> finish


