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
import sys

script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, '..', 'utils')
sys.path.append(util_dir)
# import common_utilities after setting the path for utils
import common_utilities as cu

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
    "dag_copy_data_v2", 
    default_args=default_args, 
    schedule_interval=None, 
    catchup=False, 
    max_active_runs=1,
    concurrency=10,
    template_searchpath='/home/airflow/gcs/dags/sql/'
) as dag:
    
    start_job = DummyOperator(task_id='start_job')
    end_job = DummyOperator(task_id='end_job')

    copy_var = Variable.get("hca_hrg_copy_data_v2", deserialize_json=True)
    with TaskGroup(group_id=f'TG-copy-tables') as table_grp:
        for var in copy_var:
            v_src_db = var['src_db']
            v_tgt_db = var['tgt_db']
            v_table = var['table']
            v_date = var['date']
            v_update = var['update']

        
            copy_data = BigQueryOperator(
                gcp_conn_id=config['env']['v_curated_conn_id'],
                sql = f"CALL `{config['env']['v_curated_project_id']}.{config['env']['v_hr_core_dataset_name']}`.copy_table_data_with_valid_to_date_adjustment_v2('{config['env']['v_curated_project_id']}','{v_src_db}','{v_tgt_db}','{v_table}','{v_date}', '{v_update}')",
                use_legacy_sql=False, 
                retries=0, 
                task_id = f'run_bqsql_copy_data_{v_table}'  
            )
            [copy_data]
        

start_job >> [table_grp] >>  end_job