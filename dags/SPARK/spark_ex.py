from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.variable import Variable
import pendulum


KST = pendulum.timezone('Asia/Seoul')
# SERVER_API = Variable.get("SERVER_API")
DAGS_OWNER = Variable.get('DAGS_OWNER')

default_args = {
    'owner': DAGS_OWNER,
    'depends_on_past': True,
    'start_date':datetime(2023, 9, 11, tzinfo=KST)}

dag = DAG('spark_ex',
      default_args = default_args,
      max_active_runs= 1,
      tags=['스파크 테스트'],
      schedule_interval= '@once')

sshHook1 = SSHHook(ssh_conn_id='sms-1')
sshHook2 = SSHHook(ssh_conn_id='sms-1-vpn', cmd_timeout=15)


start_task = EmptyOperator(
    task_id = 'start_task',
    dag = dag)


activate_task = SSHOperator(
    task_id = 'activate_vpn',
    command = 'sh /home/ubuntu/openvpn/activate_vpn.sh ',
    ssh_hook = sshHook1,
    do_xcom_push=False,
    dag=dag)

t1 = SSHOperator(
    task_id = 'sh_test',
    command = "ssh sparkserver 'sh /home/spark/test.sh' ",
    ssh_hook = sshHook2,
    do_xcom_push=False,
    dag=dag)

deactivate_task = SSHOperator(
    task_id = 'deactivate_vpn',
    command = 'sh /home/ubuntu/openvpn/deactivate_vpn.sh ',
    # command = 'sudo pkill openvpn ',
    ssh_hook = sshHook2,
    do_xcom_push=False,
    get_pty=True,
    dag=dag)


check_connection = BashOperator(
    task_id = 'check_original_ip_conn',
    bash_command = "nc -z -w 1 43.202.116.246 22 && echo '연결 성공' || echo '연결 실패' ",
    trigger_rule = 'none_skipped',
    dag=dag)


end_task = EmptyOperator(
    task_id = 'finish_task',
    trigger_rule='all_done',
    dag = dag)

start_task >> activate_task >> t1 >> deactivate_task >> check_connection >> end_task