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

default_args = {
    'owner': 'sms/v0.7.0',
    'depends_on_past': False,
    'start_date': datetime(2022, 8, 1, tzinfo=local_tz)
}

dag = DAG(
    'get_KOPIS',
    default_args=default_args,
    schedule="0 1 * * 1", ## 매주 월요일 AM 1:00 에 실행
    tags = ["수집","KOPIS","공연목록","공연상세정보"],
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

#정합성  체크
def check_logic(next_execution_date,**context):
    exe_dt=next_execution_date.in_timezone('Asia/Seoul').strftime('%Y-%m-%d')
    DB_CNT=context['task_instance'].xcom_pull(task_ids='Load.Kopis_to_DB')
    api_url = f"http://{SERVER_API}/check/kopis?st_dt={exe_dt}&db_cnt={DB_CNT}"
    request = requests.get(api_url)

    if request == "1" :
        return "ERROR"
    
    else:
        return "DONE"

def get_detail(next_execution_date):
    exe_dt=next_execution_date.in_timezone('Asia/Seoul').strftime('%Y-%m-%d')
    api_url = f"http://{SERVER_API}/kopis/information?date={exe_dt}"
    request = requests.get(api_url)

    print(f"{exe_dt}일, {len(request)}건 공연상세정보 적재 완료")
    print(request)



start = EmptyOperator(task_id = 'Stark.task', dag = dag)
finish = EmptyOperator(task_id = 'Finish.task', trigger_rule='one_success', dag = dag)

load_to_db = PythonOperator(task_id="Load.Kopis_to_DB",
                            python_callable=load_db,
                            dag=dag)

save_detail = PythonOperator(task_id="Save.Kopis_Detail",
                            python_callable=get_detail,
                            dag=dag)

branching = BranchPythonOperator(task_id='Check.logic',
                                 python_callable=check_logic,
                                 dag=dag)

error = EmptyOperator(task_id = 'ERROR', dag = dag)
done = EmptyOperator(task_id = 'DONE', dag = dag)


start >> load_to_db >> save_detail >> branching
branching >> error >> finish
branching >> done  >> finish



