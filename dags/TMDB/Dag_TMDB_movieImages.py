from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.models.variable import Variable
import requests
import pendulum

local_tz = pendulum.timezone("Asia/Seoul")

SERVER_API = Variable.get("SERVER_API_blah")

default_args = {
    'owner': 'sms',
    'depends_on_past': True,
    'start_date': datetime(2022, 8, 1, tzinfo=local_tz),
    'retries': 3,
    "provide_context":True,
}

dag = DAG(
    'test-tmdb-images-api',
    default_args=default_args,
    schedule="0 1 * * 5", #매주 금요일 1AM
    user_defined_macros={'local_dt': lambda execution_date: execution_date.in_timezone(local_tz).strftime("%Y-%m-%d %H:%M:%S")},
)


# 데이터 수집 API 호출
def get_api_data(api_url, **context):
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
    xcom = ti.xcom_pull(task_ids='Get.TMDB_Images_Data', key='db_counts')
    print(xcom)
    api_url_check_data = f"{SERVER_API}/check/tmdb?xcom={xcom}&category={category}&date={date}"
    response = requests.get(api_url_check_data)
    # 오류 처리
    response.raise_for_status()
    # JSON 응답 파싱
    data = response.text
    if data == '1':
        return 'ERROR'
    else:
        return 'DONE'


category = 'movieImages'
date = "{{execution_date.add(days=364, hours=9).strftime('%Y-%m-%d')}}"
api_url_get_data = f"{SERVER_API}/tmdb/movie-images?date={date}"


start = EmptyOperator(task_id = 'Start.task', dag = dag)
get_data = PythonOperator(task_id = "Get.TMDB_Images_Data", python_callable=get_api_data, op_args=[api_url_get_data], dag = dag)
finish = EmptyOperator(task_id = 'Finish.task', dag = dag)

# start >> get_data >> finish

# 정합성 체크 로직
branching = BranchPythonOperator(task_id='Check.Integrity',python_callable=check_logic, op_args=[category, date], dag=dag)
error = EmptyOperator(task_id = 'ERROR', dag = dag)
done = EmptyOperator(task_id = 'DONE', dag = dag)

start >> get_data >> branching
branching >> error
branching >> done  >> finish