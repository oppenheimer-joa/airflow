from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.variable import Variable
from airflow.models import TaskInstance
from airflow.operators.python_operator import BranchPythonOperator
import pendulum

KST = pendulum.timezone('Asia/Seoul')
DAGS_OWNER = Variable.get('DAGS_OWNER')
SERVER_HOST = Variable.get('DUCKDB_SERVER_HOST')
SCRIPT_PATH = Variable.get('DUCKDB_SCRIPT_PATH')

default_args = {
    'owner': DAGS_OWNER,
    'depends_on_past': False,
    'start_date':datetime(2023, 10, 1, tzinfo=KST)
    }

dag = DAG('load_duckdb_tmdb_movie',
      default_args = default_args,
      max_active_runs= 1,
      tags=['duckdb','데이터 로드','TMDB','People'],
      schedule="0 6 * * 5", ## 매주 금요일 AM 6:00 에 실행
      )

sshHook = SSHHook(ssh_conn_id='hooniegit', cmd_timeout=15)

def check_server():
    import subprocess
    cmd = ["nc", "-z", "-w", "1", SERVER_HOST, "22"]
    response = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    if response.returncode == 0:
        return 'connection_success'
    else:
        return 'connection_failed'


start_task = EmptyOperator(
    task_id = 'start_task',
    dag = dag)

check_server_conn = BranchPythonOperator(task_id='check_server_conn',
                                 python_callable=check_server,
                                 dag=dag)

success_conn = EmptyOperator(
    task_id = 'connection_success',
    dag = dag)

fail_conn = EmptyOperator(
    task_id = 'connection_failed',
    dag = dag)


ssh_task = SSHOperator(
    task_id = 'ssh_test',
    command = f"python3 {SCRIPT_PATH}/duck_create_tmdb_people.py",
    ssh_hook = sshHook,
    do_xcom_push=False,
    dag=dag)


end_task = EmptyOperator(
    task_id = 'finish_task',
    trigger_rule='all_done',
    dag = dag)

start_task >> check_server_conn >> [success_conn, fail_conn]
success_conn >> ssh_task >> end_task