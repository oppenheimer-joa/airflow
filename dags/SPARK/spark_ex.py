from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.variable import Variable
import pendulum, requests, json, os
from datetime import datetime


KST = pendulum.timezone('Asia/Seoul')
SMS_1 = Variable.get("SMS_1")
OPENVPN = Variable.get("OPENVPN")
# SERVER_API = Variable.get("SERVER_API")
DAGS_OWNER = Variable.get('DAGS_OWNER')


default_args = {
	'owner': DAGS_OWNER,
	'depends_on_past': True,
	'start_date':datetime(1930, 1, 1, tzinfo=KST)}

dag = DAG('spark_test',
	  default_args = default_args,
      max_active_runs= 1,
      tags=['스파크 테스트'],
      schedule_interval= '@once')   ## 매년 5월 1일 AM 11:00 실행


start_task = EmptyOperator(
	task_id = 'start_task',
	dag = dag)


activate_vpn = BashOperator(
	task_id = 'activate_openvpn',
	bash_command = """
	ssh -i /home/airflow/.ssh/sms-1.pem ubuntu@{SMS_1} 'sh /home/ubuntu/openvpn/activate.sh'
	""",
	dag=dag)

test_task = BashOperator(
	task_id = 'test_work',
	bash_command = """
	ssh -i /home/airflow/.ssh/sms-1.pem ubuntu@{OPENVPN} 'echo testtttt'
	""",
	dag=dag)

deactivate_vpn = BashOperator(
	task_id = 'deactivate_openvpn',
	bash_command = """
	ssh -i /home/airflow/.ssh/sms-1.pem ubuntu@{OPENVPN} 'sh /home/ubuntu/openvpn/deactivate.sh'
	""",
	dag=dag)

end_task = EmptyOperator(
	task_id = 'finish_task',
	dag = dag)


start_task >> activate_vpn >> test_task >> deactivate_vpn >> end_task
