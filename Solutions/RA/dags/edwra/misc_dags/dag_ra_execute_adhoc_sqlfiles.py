from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import  DummyOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage
import pendulum
import logging
import json 
import yaml
from datetime import datetime, timedelta
import os
import sys

script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, '..', 'utils')
sys.path.append(util_dir)
current_timezone = pendulum.timezone("US/Central")
import common_utilities as cu

lob = "edwra"
lob = lob.lower().strip()
lob_abbr = lob.replace("edw","")

config = cu.call_config_yaml(f"{lob_abbr}_config.yaml", f"hca_{lob_abbr}_default_vars")

def getsqlfilelist(src_folder_path) -> list:
        bucket = cu.get_bucket(config['env']['v_curated_project_id'] , config['env']['v_dag_bucket_name'])
        storage_client = storage.Client()    
        blobs = storage_client.list_blobs(bucket, prefix=src_folder_path, delimiter=None)
        blob_list = []

        #Find file/s matching pattern
        for blob in blobs:
                if ".sql" in blob.name:
                    blob_list.append(blob.name)
        
        return blob_list


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
        "param_ra_stage_dataset_name": config['env']['v_curated_project_id'] + 
                                    '.' + config['env']['v_parallon_ra_stage_dataset_name'],

        "param_ra_core_dataset_name": config['env']['v_curated_project_id'] + 
                                    '.' + config['env']['v_parallon_ra_core_dataset_name']
        # "param_ra_core_dataset_name": config['env']['v_curated_project_id'] + 
        #                             '.' + config['env']['v_parallon_ra_base_views_dataset_name']
        }
    }


with DAG(
    "dag_execute_ra_adhoc_sqlfiles", 
    default_args=default_args, 
    schedule_interval=None, 
    catchup=False, 
    max_active_runs=1, 
    max_active_tasks=10,
    template_searchpath='/home/airflow/gcs/',
    tags=["ra","misc"]
) as dag:
    
    start_job = DummyOperator(task_id='start_job')
    end_job = DummyOperator(task_id='end_job')
    
    sqlfilelist = Variable.get("ra_var_sqlfilelist",  deserialize_json=True)
    
    sql_arr=[]   
    for idx, file in enumerate(sqlfilelist):
        filename = file.replace('/', '_')
        run_adhoc_sql_task = BigQueryOperator(
                        gcp_conn_id=config['env']['v_curated_conn_id'],
                        sql = file,
                        use_legacy_sql=False, 
                        retries=0, 
                        task_id = f'run_adhoc_sql_{filename}'  
                        )
	                   
        if idx == 0:
            start_job >> run_adhoc_sql_task
        else:
            sql_arr[-1] >> run_adhoc_sql_task
        sql_arr.append(run_adhoc_sql_task) 
    
    run_adhoc_sql_task >> end_job
      
    