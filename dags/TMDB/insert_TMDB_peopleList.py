from datetime import datetime, timedelta
import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.models.variable import Variable

local_tz = pendulum.timezone('Asia/Seoul')

# 프로젝트의 모든 DAG 공통 사항 기재
default_args = {
    "owner" : "sms",
    "depends_on_past" : True
}

# 프로젝트마다 변동될 DAG 사항들 기재
dag = DAG(
    dag_id='mysql_tmdb/peopleID_API-01',
    description='update MySQL databases\' movie list',
    tags=['수집', 'TMDB', 'MySQL', 'peopleID'],
    max_active_runs=1, 
    concurrency=1,
    start_date=datetime(year=2022, month=8, day=1, hour=0, minute=0, tzinfo=local_tz),
    schedule_interval='0 0 * * 5',
    default_args=default_args
)

# Airflow Variables
SERVER_API = Variable.get_val["SERVER_API"]
date = "{{execution_date.add(days=364, hours=9).strftime('%Y-%m-%d')}}"

# start
start = EmptyOperator(
    task_id="start",
    dag=dag
)

# insert
# curl -X GET http://{SERVER_API}/tmdb/mysql-people?date=2023-08-25
insert = BashOperator(
    task_id="insert",
    bash_command=f'''
    curl -X GET http://{SERVER_API}/mysql-people?date={date}
    ''',
    dag=dag
)

# end
end = EmptyOperator(
    task_id="end",
    dag=dag
)

start >> insert >> end