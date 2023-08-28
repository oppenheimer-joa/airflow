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
ST_DT = datetime.now().strftime('%Y-%m-%d')

default_args = {
    'owner': 'sms',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 28, tzinfo=local_tz),
    'retries': 0,
}

dag = DAG(
    'get-kopis-api',
    default_args=default_args,
    schedule="0 1 * * 1", #매주 월요일 1AM
    user_defined_macros={'local_dt': lambda execution_date: execution_date.in_timezone(local_tz).strftime("%Y-%m-%d %H:%M:%S")},
)


def load_db(**context):
    api_url = f"http://{SERVER_API}/kopis/performance-to-db/"
    response = requests.get(api_url).json()

    return response

start = EmptyOperator(task_id = 'Stark.task', dag = dag)
finish = EmptyOperator(task_id = 'Finish.task', dag = dag)

load_to_db = PythonOperator(task_id="Load.Kopis_to_DB",
                            python_callable=load_db,
                            dag=dag)

save_detail = BashOperator(task_id="Save.Kopis_Detail",
                            bash_command=f"curl {SERVER_API}/kopis/information",
                            dag=dag)


#정합성  체크
def check_logic(**context):
    DB_CNT=context['task_instance'].xcom_pull(task_ids='Load.Kopis_to_DB')
    api_url = f"http://{SERVER_API}/check/kopis?st_dt={ST_DT}&db_cnt={DB_CNT}"
    request = requests.get(api_url)

    if request == 1 :
        return "ERROR"
    
    else:
        return "DONE"
    
branching = BranchPythonOperator(task_id='Check.logic',
                                 python_callable=check_logic,
                                 dag=dag)

error = EmptyOperator(task_id = 'ERROR', dag = dag)
done = EmptyOperator(task_id = 'DONE', dag = dag)


start >> load_to_db >> save_detail >> branching
branching >> error >> finish
branching >> done  >> finish



