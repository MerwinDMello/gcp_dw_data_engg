from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'hca_hrg_atos',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 18),
    'email': None,
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=480)
    
}

with DAG(
    dag_id='dag_adhoc_bashoperator',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["adhoc"]
    
) as dag: 
    
    start_job = DummyOperator(task_id='start_dag')
    end_job = DummyOperator(task_id='end_dag')
    
    var_dfjobid = Variable.get("var_dfjobid")
    
    adhoc_bash_command = BashOperator(
                    task_id="run_adhoc_bash_command",
                    dag = dag,
                    bash_command='gcloud dataflow jobs cancel --region=us-east4 {}'.format(var_dfjobid)
                    )
                    
            
        
    start_job >> adhoc_bash_command >> end_job