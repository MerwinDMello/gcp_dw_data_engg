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
ddl_dir = os.path.join(script_dir, '..', 'sql/ddl')
sys.path.append(util_dir)
# import common_utilities after setting the path for utils
import common_utilities as cu

config = cu.call_config_yaml("hrg_config.yaml","hca_hrg_default_vars")
bq_project_id = config['env']['v_curated_project_id']
env_name = config['env']['v_env_name']

default_args = {
    'owner': 'hca_hrg_atos',
    'depends_on_past': False,
    'start_date': datetime(2023,2,13),
    'email': config['env']['v_failure_email_list'],
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=240),
    #params to substitute in sql files
    'params': {
        "param_hr_stage_dataset_name": bq_project_id + '.' +  config['env']['v_hr_stage_dataset_name'],
        "param_hr_core_dataset_name": bq_project_id + '.' + config['env']['v_hr_core_dataset_name'],
        "param_hr_base_views_dataset_name": bq_project_id + '.' + config['env']['v_hr_base_views_dataset_name'],
        "param_hr_views_dataset_name": bq_project_id + '.' + config['env']['v_hr_views_dataset_name'],
        "param_pub_views_dataset_name": bq_project_id + '.' + config['env']['v_pub_views_dataset_name'],
        "param_fs_core_dataset_name": bq_project_id + '.' + config['env']['v_fs_core_dataset_name'],
        "param_fs_base_views_dataset_name": bq_project_id + '.' + config['env']['v_fs_base_views_dataset_name'],
        "param_hr_audit_dataset_name": bq_project_id + '.' + config['env']['v_hr_audit_dataset_name'],
        "param_auth_base_views_dataset_name": bq_project_id + '.' + config['env']['v_auth_base_views_dataset_name'],
        "param_hr_bi_views_dataset_name": bq_project_id + '.' + config['env']['v_hr_bi_views_dataset_name'],
        "param_hr_krs_views_dataset_name": bq_project_id + '.' + config['env']['v_hr_krs_views_dataset_name'],
        "param_hr_rstc_views_dataset_name": bq_project_id + '.' + config['env']['v_hr_rstc_views_dataset_name'],
        "param_hr_stnd_views_dataset_name": bq_project_id + '.' + config['env']['v_hr_stnd_views_dataset_name'],
        "param_hr_lmtd_views_dataset_name": bq_project_id + '.' + config['env']['v_hr_lmtd_views_dataset_name'],
        "param_sec_base_views_dataset_name": bq_project_id + '.' + config['env']['v_sec_base_views_dataset_name'],
        "param_pf_core_dataset_name": bq_project_id + '.' + config['env']['v_pf_core_dataset_name'],
        "param_pf_base_views_dataset_name": bq_project_id + '.' + config['env']['v_pf_base_views_dataset_name'],
        "param_ga_core_dataset_name": bq_project_id + '.' + config['env']['v_ga_core_dataset_name'],
        "param_ga_base_views_dataset_name": bq_project_id + '.' + config['env']['v_ga_base_views_dataset_name']

        }
    }

with DAG(
    "dag_create_db_objects_parallel", 
    default_args=default_args, 
    schedule_interval=None, 
    catchup=False, 
    max_active_runs=1, 
    template_searchpath='/home/airflow/gcs/dags/sql/'
) as dag:
    
    start_job = DummyOperator(task_id='start_job')
    end_job = DummyOperator(task_id='end_job')

    dataset_list = os.listdir(ddl_dir)

    create_objects_var = Variable.get("hca_hrg_create_db_objects", deserialize_json=True)

    arr=[]
    idx = 0
    for dataset in dataset_list:
        
        if dataset.find('_') != -1:
            if dataset.rsplit("_",1)[1].lower() == "views":
                object_type = "VIEW"
            else:
                object_type = "TABLE"
        else:
            object_type = "TABLE"
        create_objects_for_dataset = list(filter(lambda d: d['dataset'] == dataset, create_objects_var))
        create_objects_for_dataset_files = [f"ddl/{dataset}/{obj['object_name']}.sql" for obj in create_objects_for_dataset]
        drop_tables_for_dataset = list(filter(lambda d: d['drop_object'] == "TRUE", create_objects_for_dataset))
        drop_tables_for_dataset_list = [f"DROP {object_type} IF EXISTS `{bq_project_id}.{dataset}.{obj['object_name']}`;" for obj in drop_tables_for_dataset]

        if len(create_objects_for_dataset_files) > 0:
            with TaskGroup(group_id=f'TG-create-db_objects-{dataset}') as db_object_grp:
                create_object = BigQueryOperator(
                            gcp_conn_id=bq_project_id,
                            sql = create_objects_for_dataset_files,
                            use_legacy_sql=False, 
                            retries=0, 
                            task_id = f'run_bqsql_create_object_{dataset}'  
                )
                if len(drop_tables_for_dataset_list) > 0:        
                    drop_object = BigQueryOperator(
                        gcp_conn_id=bq_project_id,
                        sql = drop_tables_for_dataset_list,
                        use_legacy_sql=False, 
                        retries=0, 
                        task_id = f'run_bqsql_drop_object_{dataset}'  
                    )
                    drop_object >> create_object
                else:
                    create_object
        
            if idx == 0:
                start_job >> db_object_grp
            # elif idx == len(dataset_list):
            #     db_object_grp >> end_job
            else:
                arr[-1] >> db_object_grp
            
            arr.append(db_object_grp)
            idx += 1
    if idx > 0:
        arr[-1] >> end_job
            # start_job >> [db_object_grp] >>  end_job