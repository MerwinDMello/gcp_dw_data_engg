from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'hca_pbs_atos',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 18),
    'email': None,
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=20)
    
}

with DAG(
    dag_id='dag_test_df',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["adhoc"]
    
) as dag: 
    
    start_job = DummyOperator(task_id='start_dag')
    end_job = DummyOperator(task_id='end_dag')
    
    run_df_command = BashOperator(
                    task_id="run_df_job",
                    dag = dag,
                    bash_command='python /home/airflow/gcs/dags/scripts/test_df.py --config_file=edwpbs/config_df.yaml'
                    )
        
    start_job >> run_df_command >> end_job