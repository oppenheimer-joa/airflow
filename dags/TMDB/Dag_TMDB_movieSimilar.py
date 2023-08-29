from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.models.variable import Variable
import requests
import pendulum

local_tz = pendulum.timezone("Asia/Seoul")

SERVER_API = Variable.get("SERVER_API")

default_args = {
    'owner': 'sms',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 24, tzinfo=local_tz),
    'retries': 0,
}

dag = DAG(
    'test-tmdb-similar-api',
    default_args=default_args,
    schedule="0 1 * * 1", #매주 월요일 1AM
    user_defined_macros={'local_dt': lambda execution_date: execution_date.in_timezone(local_tz).strftime("%Y-%m-%d %H:%M:%S")},
)


# 데이터 수집 API 호출
def get_api_data(task_name, api_url):
    cmd = f"curl -X 'GET' {api_url} -H 'accept: application/json'"
    bash_task = BashOperator(
                            task_id=task_name,
                            bash_command=cmd,
                            dag=dag)

    return bash_task


# # 정합성  체크
# def check_logic(ti):
#     # XCom에서 값을 가져옵니다.
#     xcom = ti.xcom_pull(task_ids='Get.TMDB_Simila_Data')
#     category = 'movieSimilar'
#     date = 

date = "{{execution_date.add(hours=9).strftime('%Y-%m-%d')}}"
api_url = f"{SERVER_API}/tmdb/movie-similar?date={date}"


start = EmptyOperator(task_id = 'Start.task', dag = dag)
get_data = get_api_data("Get.TMDB_Similar_Data", api_url=api_url)
finish = EmptyOperator(task_id = 'Finish.task', dag = dag)

start >> get_data >> finish

# # 정합성 체크 로직
# branching = BranchPythonOperator(task_id='Check.Integrity',python_callable=check_logic, op_args=[f"{SERVER_API}/kobis/daily-boxoffice"], dag=dag)
# error = EmptyOperator(task_id = 'ERROR', dag = dag)
# done = EmptyOperator(task_id = 'DONE', dag = dag)
# 
# start >> get_data >> branching
# branching >> error >> finish
# branching >> done  >> finish