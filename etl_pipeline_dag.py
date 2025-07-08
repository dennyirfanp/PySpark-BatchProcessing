from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime

default_args = {
    'owner': 'student',
    'start_date': datetime(2025, 7, 1),
    'retries': 1
}

with DAG(
    dag_id='pyspark_batch_etl_pipeline',
    default_args=default_args,
    description='Run batch ETL process using PySpark and store to PostgreSQL',
    schedule_interval='@daily',
    catchup=False,
    tags=['pyspark', 'postgres', 'etl']
) as dag:

    create_table = PostgresOperator(
        task_id='create_flights_summary_table',
        postgres_conn_id='postgres_default',
        sql='/sql/create_flights_summary.sql'
    )

    run_etl_pyspark = BashOperator(
        task_id='run_etl_pyspark',
        bash_command='spark-submit /opt/airflow/scripts/etl_pyspark.py'
    )

    load_to_postgres = PostgresOperator(
        task_id='load_to_postgres',
        postgres_conn_id='postgres_default',
        sql='/sql/insert_queries.sql'
    )

    create_table >> run_etl_pyspark >> load_to_postgres
