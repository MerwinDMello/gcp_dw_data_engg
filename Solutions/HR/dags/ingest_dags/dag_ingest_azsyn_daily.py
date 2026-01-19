from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime, timedelta
import os
import sys
import time
from airflow.utils.task_group import TaskGroup

script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, '..', 'utils')
sys.path.append(util_dir)
import common_utilities as cu

config = cu.call_config_yaml("hrg_config.yaml", "hca_hrg_default_vars")
srcsys_config = cu.call_config_yaml("azsyn_config.yaml", "azsyn_vars")

default_args = {
    'owner': 'hca_hrg_atos',
    'depends_on_past': False,
    'email': config['env']['v_failure_email_list'],
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=240)
}

with DAG(
    dag_id="dag_ingest_azsyn_daily_tbllist",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval=srcsys_config['v_schedule_interval'],
    catchup=False,
    max_active_runs=1,
    tags=["azsyn", "daily"]
) as dag:

    run_dataflow_job = BashOperator(
                task_id="run_azure_synapse_dataflow_job",
                dag = dag,
                bash_command='python /home/airflow/gcs/dags/scripts/{} --src_sys_config_file=azsyn_config.yaml --src_sys_airflow_varname=azsyn_vars --src_tbl_list=srctablelist'.format(srcsys_config['v_jdbcbqpytemplate'])
                )
        
    run_dataflow_job
