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
util_dir = os.path.join(script_dir, '..', '..','utils')
sys.path.append(util_dir)
# import common_utilities after setting the path for utils
import common_utilities as cu

config_lob = Variable.get("hca_ops_config_lob")
config_lob_suffix = config_lob.replace("edw","")
if config_lob_suffix == "pi":
    config = cu.call_config_yaml(f"config/{config_lob}/{config_lob_suffix}_config.yaml",f"hca_{config_lob_suffix}_default_vars")
else:
    config = cu.call_config_yaml(f"{config_lob}/config/{config_lob_suffix}_config.yaml",f"hca_{config_lob_suffix}_default_vars")
gcp_conn_id = config['env']['v_curated_conn_id']
bq_project_id = config['env']['v_curated_project_id']
bq_mirrored_project_id = config['env']['v_mirroring_project_id']
params = {
    f"param_{config_lob_suffix}_stage_dataset_name": bq_project_id + '.' +  config['env'][f'v_{config_lob_suffix}_stage_dataset_name'],
    f"param_{config_lob_suffix}_views_dataset_name": bq_project_id + '.' + config['env'][f'v_{config_lob_suffix}_views_dataset_name'],
    f"param_{config_lob_suffix}_audit_dataset_name": bq_project_id + '.' + config['env'][f'v_{config_lob_suffix}_audit_dataset_name'],
    f"param_{config_lob_suffix}_mirrored_core_dataset_name": bq_mirrored_project_id + '.' + config['env'][f'v_{config_lob_suffix}_mirrored_core_dataset_name'],
    f"param_{config_lob_suffix}_mirrored_base_views_dataset_name": bq_project_id + '.' + config['env'][f'v_{config_lob_suffix}_mirrored_base_views_dataset_name'],
    f"param_pub_views_dataset_name": bq_project_id + '.' + config['env']['v_pub_views_dataset_name'],
    f"param_auth_base_views_dataset_name": bq_project_id + '.' + config['env']['v_auth_base_views_dataset_name'],
    f"param_sec_core_dataset_name": bq_project_id + '.' + config['env']['v_sec_core_dataset_name'],
    f"param_sec_base_views_dataset_name": bq_project_id + '.' + config['env']['v_sec_base_views_dataset_name'],
    f"param_monitoring_views_dataset_name": bq_project_id + '.' + config['env']['v_monitoring_views_dataset_name']
}
cr_mirroring = Variable.get("hca_cr_mirroring")
if cr_mirroring.strip().upper() == "TRUE":
    params.update({
        f"param_{config_lob_suffix}_core_dataset_name": bq_mirrored_project_id + '.' + config['env'][f'v_{config_lob_suffix}_mirrored_core_dataset_name'],
        f"param_{config_lob_suffix}_base_views_dataset_name": bq_project_id + '.' + config['env'][f'v_{config_lob_suffix}_mirrored_base_views_dataset_name']
    })
else:
    params.update({
        f"param_{config_lob_suffix}_core_dataset_name": bq_project_id + '.' + config['env'][f'v_{config_lob_suffix}_core_dataset_name'],
        f"param_{config_lob_suffix}_base_views_dataset_name": bq_project_id + '.' + config['env'][f'v_{config_lob_suffix}_base_views_dataset_name']
    })

if config_lob_suffix == "cr":
    params.update({
        f"param_{config_lob_suffix}_bi_views_dataset_name": bq_project_id + '.' + config['env'][f'v_{config_lob_suffix}_bi_views_dataset_name'],
        f"param_pf_staging_dataset_name": bq_project_id + '.' + config['env']['v_pf_staging_dataset_name'],
        f"param_cp_base_views_dataset_name": bq_project_id + '.' + config['env']['v_cp_base_views_dataset_name'],
        f"param_pf_base_views_dataset_name": bq_project_id + '.' + config['env']['v_pf_base_views_dataset_name']
    })

env_name = config['env']['v_env_name']

default_args = {
    'owner': 'hca_hrg_atos',
    'depends_on_past': False,
    'start_date': datetime(2023,2,28),
    'email': config['env']['v_failure_email_list'],
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=240),
    #params to substitute in sql files 
    'params': params
    }

with DAG(
    "dag_create_db_objects_parallel_cr", 
    default_args=default_args, 
    schedule_interval=None, 
    catchup=False, 
    max_active_runs=1, 
    template_searchpath='/home/airflow/gcs/dags/',
    tags=['cr']
) as dag:
    
    start_job = DummyOperator(task_id='start_job')
    end_job = DummyOperator(task_id='end_job')

    if config_lob_suffix == "cr":
        dataset_list =['auth_base_views',f'edw{config_lob_suffix}_staging',f'edw{config_lob_suffix}',f'edw{config_lob_suffix}_base_views',f'edw{config_lob_suffix}_views',f'edw{config_lob_suffix}_bi_views',f'edw{config_lob_suffix}_ac','edw_pub_views','edwops_ac']
    else:
        dataset_list =['auth_base_views',f'edw{config_lob_suffix}_staging',f'edw{config_lob_suffix}',f'edw{config_lob_suffix}_base_views',f'edw{config_lob_suffix}_views',f'edw{config_lob_suffix}_ac','edw_pub_views','edwops_ac']

    create_objects_var = Variable.get(f"hca_{config_lob_suffix}_create_db_objects", deserialize_json=True)

    arr=[]
    idx = 0
    for dataset in dataset_list:
        if env_name == "dev":
            if cr_mirroring.strip().upper() != "TRUE":
                if "base_views" in dataset:
                    v_copy_dataset = "_copy"
                else:
                    v_copy_dataset = ""
            else:
                v_copy_dataset = ""
        else:
            if cr_mirroring.strip().upper() == "TRUE":
                if "base_views" in dataset:
                    v_copy_dataset = "_copy"
                else:
                    v_copy_dataset = ""
            else:
                v_copy_dataset = ""

        create_objects_for_dataset = list(filter(lambda d: d['dataset'] == dataset, create_objects_var))
        if config_lob_suffix == "pi":
            create_objects_for_dataset_files = [f"sql/ddl/{config_lob}/{dataset}/{obj['object_name']}.sql" for obj in create_objects_for_dataset]
        else:
            create_objects_for_dataset_files = [f"{config_lob}/sql/ddl/{dataset}/{obj['object_name']}.sql" for obj in create_objects_for_dataset]
        if "view" not in dataset.lower():
            drop_tables_for_dataset = list(filter(lambda d: d['drop_object'] == "TRUE", create_objects_for_dataset))
            drop_tables_for_dataset_list = [f"DROP TABLE IF EXISTS `{bq_project_id}.{dataset}{v_copy_dataset}.{obj['object_name']}`;" for obj in drop_tables_for_dataset]
        else:
            drop_tables_for_dataset = []
            drop_tables_for_dataset_list = []

        if len(create_objects_for_dataset_files) > 0:
            with TaskGroup(group_id=f'TG-create-db_objects-{dataset}{v_copy_dataset}') as db_object_grp:
                create_object = BigQueryOperator(
                            gcp_conn_id=gcp_conn_id,
                            sql = create_objects_for_dataset_files,
                            use_legacy_sql=False, 
                            retries=0, 
                            task_id = f'run_bqsql_create_object_{dataset}{v_copy_dataset}'  
                )
                if len(drop_tables_for_dataset_list) > 0:        
                    drop_object = BigQueryOperator(
                        gcp_conn_id=gcp_conn_id,
                        sql = drop_tables_for_dataset_list,
                        use_legacy_sql=False, 
                        retries=0, 
                        task_id = f'run_bqsql_drop_object_{dataset}{v_copy_dataset}'  
                    )
                    drop_object >> create_object
                else:
                    create_object
        
            if idx == 0:
                start_job >> db_object_grp
            else:
                arr[-1] >> db_object_grp
            
            arr.append(db_object_grp)
            idx += 1
    if idx > 0:
        arr[-1] >> end_job
