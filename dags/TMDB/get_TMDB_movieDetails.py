from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.models.variable import Variable
import requests
import pendulum

local_tz = pendulum.timezone("Asia/Seoul")

SERVER_API = Variable.get("SERVER_API")
category = 'movieDetails'
date = "{{execution_date.add(days=182, hours=9).strftime('%Y-%m-%d')}}"

default_args = {
    'owner': 'sms/v0.7.0',
    'depends_on_past': True,
    'start_date': datetime(1999, 7, 3, tzinfo=local_tz),
    "provide_context":True,
}

dag = DAG(
    dag_id='load_tmdb_detail_API-01',
    default_args=default_args,
    schedule="0 3 * * 5", ## 990703+182일~ 매주 금요일 AM 03:00 실행
    tags =["수집","TMDB","영화상세정보"],
    max_active_runs= 1
    )

# 데이터 수집 API 호출
def get_api_data(**context):
    api_url_get_data = f"http://{SERVER_API}/tmdb/movie-details?date={date}"
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


# # 정합성  체크
def check_logic(category, date, **context):
    # XCom에서 값을 가져옵니다.
    ti = context['ti']
    xcom = ti.xcom_pull(task_ids='Get.TMDB_Details_Data', key='db_counts')
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

    base_url = f"http://{SERVER_API}/cleansing/detail"
    curl_url = f"{base_url}?target_date={target_date}"
    command = ["curl", curl_url]

    try:
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
        print("Curl command output:", result.stdout)
    except subprocess.CalledProcessError as e:
        print("err:", e.stderr)

start = EmptyOperator(task_id = 'Start.task', dag = dag)
get_data = PythonOperator(task_id = "Get.TMDB_Details_Data", python_callable=get_api_data, dag = dag)
finish = EmptyOperator(task_id = 'Finish.task', dag = dag)

# start >> get_data >> finish

# 정합성 체크 로직
branching = BranchPythonOperator(task_id='Check.Integrity',python_callable=check_logic, op_args=[category, date], dag=dag)

cleansing_data = PythonOperator(
    task_id = 'delete.TMDB.movieDetails.datas',
    python_callable=erase_loaded_data,
    op_args=['{{next_execution_date.strftime("%Y-%m-%d")}}'],
    dag = dag)

error = EmptyOperator(task_id = 'ERROR', dag = dag)
done = EmptyOperator(task_id = 'DONE', dag = dag)

start >> get_data >> branching
branching >> error
branching >> done  >> finish