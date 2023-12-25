from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Default args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'first_pyspark_dag',
    default_args=default_args,
    description='DAG to schedule PySpark jobs',
    schedule='@daily'
)

# Task to run job1.py
user_count_task = BashOperator(
    task_id='user_count_task',
    bash_command='set -x && spark-submit spark/src/main/python/com/example/user_count.py',
    dag=dag,
)

# Task to run job1.py
product_count_task = BashOperator(
    task_id='product_count_task',
    bash_command='set -x && spark-submit spark/src/main/python/com/example/product_count.py',
    dag=dag,
)

# Define task dependencies
user_count_task >> product_count_task

# Optionally, you can add more tasks or customize the DAG as needed

if __name__ == "__main__":
    dag.cli()