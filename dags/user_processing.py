from airflow.models import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator

default_args = {
    'start_date': datetime(2022, 1, 9)
}

with DAG('user_processing', schedule_interval='@daily',
         default_args=default_args, catchup=False) as dag:
    creating_table = PostgresOperator(
        task_id='creating_table',
        postgres_conn_id='db_psql',
        sql='''
            CREATE TABLE IF NOT EXISTS user_info(
                email varchar(100) NOT NULL PRIMARY KEY,
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                gender TEXT NOT NULL
            )
        '''
    )

    bash_test = BashOperator(
        task_id='bash_test',
        bash_command='echo "test"',
    )
