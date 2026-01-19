from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator

from datetime import timedelta
import pendulum

current_timezone = pendulum.timezone("US/Central")

default_args = {
    'owner': 'hca_hrg_atos',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2023, 8, 19, tz=current_timezone),
    'email': [],
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=10)
    }

def get_date(**kwargs):
    now = pendulum.now(current_timezone)
    today = int(now.format('YYYYMMDD'))
    ti = kwargs['ti']
    ti.xcom_push(key='current_date', value=today)
    #return today
    return None

def display_date(**kwargs):
    ti = kwargs['ti']
    #today = ti.xcom_pull(task_ids='first_py_task')
    today = ti.xcom_pull(key='current_date', task_ids=['first_py_task'])
    print("Today is " + str(today))
    return None

dag = DAG(
        dag_id='test_dag_xcom',
        default_args=default_args, 
        start_date = pendulum.datetime(2023, 8, 19, tz=current_timezone),
        schedule_interval=None,
        catchup=False, 
        max_active_runs=1, 
        template_searchpath='/home/airflow/gcs/dags/sql/',
        tags=['XCOM']
    )
    
with dag:
    start_job = DummyOperator(task_id='start_dag')
    end_job = DummyOperator(task_id='end_dag')
    
    first_py_task= PythonOperator(
                    task_id = 'first_py_task',
                    python_callable = get_date,
                    provide_context = True
                    )
    
    second_py_task= PythonOperator(
                    task_id = 'second_py_task',
                    python_callable = display_date,
                    provide_context = True
                    )
                    
    start_job >> first_py_task >> second_py_task >> end_job