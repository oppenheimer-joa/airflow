from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.models.variable import Variable
import pendulum
import requests

local_tz = pendulum.timezone("Asia/Seoul")
SERVER_API = Variable.get("SERVER_API")
DAGS_OWNER = Variable.get('DAGS_OWNER')

default_args = {
    'owner': DAGS_OWNER,
    'depends_on_past': True,
    'start_date': datetime(2023, 8, 1, tzinfo=local_tz)
}

dag = DAG(
    'load_KOPIS_performance_API-01',
    default_args=default_args,
    schedule="0 1 * * 1", ## 매주 월요일 AM 1:00 에 실행
    tags = ["수집","KOPIS","공연상세정보"],
    max_active_runs= 1,
    user_defined_macros={'local_dt': lambda execution_date: execution_date.in_timezone(local_tz).strftime("%Y-%m-%d %H:%M:%S")},
)

#DB 적재
def load_db(next_execution_date,**context):
    exe_dt=next_execution_date.in_timezone('Asia/Seoul').strftime('%Y-%m-%d')
    api_url = f"http://{SERVER_API}/kopis/performance-to-db?date={exe_dt}"

    response = requests.get(api_url).json()

    print(f"{exe_dt}일, {response}건 performance DB 적재 완료")

    return response

#정합성 체크
def check_logic(next_execution_date,**context):
    exe_dt=next_execution_date.in_timezone('Asia/Seoul').strftime('%Y-%m-%d')
    DB_CNT=context['task_instance'].xcom_pull(task_ids='load_kopis_to_DB')
    api_url = f"http://{SERVER_API}/check/kopis?st_dt={exe_dt}&db_cnt={DB_CNT}"
    request = requests.get(api_url).json()

    if request == "1" :
        return "ERROR"
    
    else:
        return "DONE"

def get_detail(next_execution_date):
    exe_dt=next_execution_date.in_timezone('Asia/Seoul').strftime('%Y-%m-%d')
    api_url = f"http://{SERVER_API}/kopis/information?date={exe_dt}"
    request = requests.get(api_url).json()

    print(f"{exe_dt}일 공연상세정보 적재 완료")
    print(request)

# 데이터 s3에 넣기
def blob_data(next_execution_date):
    exe_dt=next_execution_date.in_timezone('Asia/Seoul').strftime('%Y-%m-%d')
    import subprocess
    curl_url = f"http://{SERVER_API}/blob/kopis?st_dt={exe_dt}"
    command = ["curl", curl_url]
    subprocess.run(command)
  
def erase_loaded_data(next_execution_date):
    exe_dt=next_execution_date.in_timezone('Asia/Seoul').strftime('%Y-%m-%d')
    api_url = f"http://{SERVER_API}/cleansing/kopis?ST_DT={exe_dt}"
    request = requests.get(api_url).json()
    print(request)



start = EmptyOperator(task_id = 'start_kopis_data_task', dag = dag)

load_to_db = PythonOperator(task_id="load_kopis_to_DB",
                            python_callable=load_db,
                            dag=dag)

save_detail = PythonOperator(task_id="save_kopis_detail",
                            python_callable=get_detail,
                            dag=dag)

branching = BranchPythonOperator(task_id='check_integrity',
                                 python_callable=check_logic,
                                 dag=dag)

error = EmptyOperator(task_id = 'ERROR', dag = dag)
done = EmptyOperator(task_id = 'DONE', dag = dag)

# blob 로직
push_data = PythonOperator(task_id = "push_kopis_datas",
                           python_callable=blob_data,
                           dag = dag)

cleansing_data = PythonOperator(task_id = 'delete_kopis_datas',
                                python_callable=erase_loaded_data,
                                dag = dag)

finish = EmptyOperator(task_id = 'finish_kopis_data_task', trigger_rule='one_success', dag = dag)

start >> load_to_db >> save_detail >> branching
branching >> [error, done]
done >> push_data >> cleansing_data >> finish
error