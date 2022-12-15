#Import dependencies 
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Python logic to derive yetsreday's date
yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

# Default arguments
default_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Python custom logic/function for python callables
def print_hello():
    print('Hey I am Python operator')

# DAG definitions 
with DAG(dag_id='bash_python_operator_demo',
         catchup=False,
         schedule_interval=timedelta(days=1),
         default_args=default_args
         ) as dag:

    # Tasks starts here 

    # Dummy Start task
    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

    # Bash operator , task  
    bash_task = BashOperator(
    task_id='bash_task',
    bash_command="date;echo 'Hey I am bash operator'",
    )
    # Python operator , task
    pyhon_task = PythonOperator(
    task_id='pyhon_task',
    python_callable=print_hello,
    dag=dag)

    # Dummy end task
    end = DummyOperator(
        task_id='end',
        dag=dag,
    )

# Setting up Task dependencies using Airflow standard notations        
start >>  bash_task >> pyhon_task >> end
