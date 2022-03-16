from airflow.models import DAG
from datetime import datetime
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.operators.bash import BashOperator

default_args = {
    'start_date': datetime(2022, 3, 10)
}

CREATE_TABLE_SQL_STRING = (
    f"""create table if not exists ANNUAL_TICKET_SALES(
            ticket_year BIGINT,
            tickets_sold BIGINT,
            total_box_office BIGINT,
            total_inflation_adjusted_office BIGINT,
            average_ticket_price BIGINT
        )"""
)

DBT_DIR = "/Users/yunpeng.ding/Documents/dbt_development"


with DAG('annual_ticket_processing',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=True) as dag:

    snowflake_create_table = SnowflakeOperator(
        task_id='snowflake_create_table',
        snowflake_conn_id='snowflake_connection',
        sql=CREATE_TABLE_SQL_STRING,
        database='PROD',
        schema='RAW',
    )

    copy_into_table = S3ToSnowflakeOperator(
        task_id='copy_into_table',
        snowflake_conn_id='snowflake_connection',
        table='ANNUAL_TICKET_SALES',
        schema='RAW',
        stage='S3_TICKET_SALES',
        file_format="(type = 'CSV',field_delimiter = ',',SKIP_HEADER = 1)"
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'''
            cd {DBT_DIR} &&
            docker run --rm -v $(pwd):/usr/app -v $(pwd):/root/.dbt fishtownanalytics/dbt:0.19.0 test
        '''
    )

    transform_data = BashOperator(
        task_id='transform_data',
        bash_command=f'''
            cd {DBT_DIR} &&
            docker run --rm -v $(pwd):/usr/app -v $(pwd):/root/.dbt fishtownanalytics/dbt:0.19.0 run
        '''
    )

snowflake_create_table >> copy_into_table >> dbt_test >> transform_data
