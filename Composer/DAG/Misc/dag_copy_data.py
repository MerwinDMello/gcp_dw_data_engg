from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import  DummyOperator
from utils import common_utilities as cu
import json 
import yaml
from datetime import datetime, timedelta
import os

config = cu.call_config_yaml("hrg_config.yaml","hca_hrg_default_vars")

default_args = {
    'owner': 'hca_hrg_atos',
    'depends_on_past': False,
    'start_date': datetime(2022,11,18),
    'email': config['env']['v_failure_email_list'],
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=240),
    #params to substitute in sql files
    'params': {
        "param_hr_stage_dataset_name": config['env']['v_curated_project_id'] + 
                                    '.' +  config['env']['v_hr_stage_dataset_name'],
        "param_hr_core_dataset_name": config['env']['v_curated_project_id'] + 
                                    '.' + config['env']['v_hr_core_dataset_name'],
        }
    }


with DAG(
    "dag_copy_data", 
    default_args=default_args, 
    schedule_interval=None, 
    catchup=False, 
    max_active_runs=1, 
    template_searchpath='/home/airflow/gcs/dags/sql/'
) as dag:
    
    start_job = DummyOperator(task_id='start_job')
    end_job = DummyOperator(task_id='end_job')

    copy_var = Variable.get("hca_hrg_copy_data", deserialize_json=True)
    with TaskGroup(group_id=f'TG-copy-tables') as table_grp:
        for var in copy_var:
            v_db = var['db']
            v_table = var['table']
            v_date = var['date']
            v_update = var['update']
            v_use_copy_dataset = var['use_copy_dataset']

        
            copy_data = BigQueryOperator(
                gcp_conn_id=config['env']['v_curated_conn_id'],
                sql = f"CALL `{config['env']['v_curated_project_id']}.{config['env']['v_hr_core_dataset_name']}`.copy_table_data_with_valid_to_date_adjustment('{config['env']['v_curated_project_id']}','{v_db}','{v_table}','{v_date}', '{v_update}', {v_use_copy_dataset})",
                use_legacy_sql=False, 
                retries=0, 
                task_id = f'run_bqsql_copy_data_{v_table}'  
            )
            [copy_data]
        

start_job >> [table_grp] >>  end_job