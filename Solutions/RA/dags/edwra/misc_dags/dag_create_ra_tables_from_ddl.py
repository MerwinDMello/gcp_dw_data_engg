from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
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
from google.cloud import storage

config = cu.call_config_yaml("ra_config.yaml", "hca_ra_default_vars")
bq_project_id = config['env']['v_curated_project_id']


def getsqlfilelist(src_folder_path) -> list:
    bucket = cu.get_bucket(config['env']['v_curated_project_id'], config['env']['v_dag_bucket_name'])
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket, prefix=src_folder_path, delimiter=None)
    blob_list = []

    # Find file/s matching pattern
    for blob in blobs:
        blob_list.append(blob.name)

    print(blob_list)

    return blob_list


default_args = {
    'owner': 'hca_ra_atos',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 24),
    'email': config['env']['v_failure_email_list'],
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=240),
    # params to substitute in sql files
    'params': {
        "param_parallon_ra_stage_dataset_name": bq_project_id + '.' + config['env']['v_parallon_ra_stage_dataset_name'],
        "param_parallon_ra_core_dataset_name": bq_project_id + '.' + config['env']['v_parallon_ra_core_dataset_name'],
        "param_parallon_ra_base_views_dataset_name": bq_project_id + '.' + config['env']['v_parallon_ra_base_views_dataset_name'],
        "param_parallon_ra_views_dataset_name": bq_project_id + '.' + config['env']['v_parallon_ra_views_dataset_name'],
        "param_parallon_pbs_views_dataset_name": bq_project_id + '.' + config['env']['v_parallon_pbs_views_dataset_name'],
        "param_parallon_pbs_base_views_dataset_name": bq_project_id + '.' + config['env']['v_parallon_pbs_base_views_dataset_name'],
        "param_parallon_edwcm_views_dataset_name": bq_project_id + '.' + config['env']['v_clinical_cm_views_dataset_name'],
        "param_bqutil_fns_dataset_name": '`' + bq_project_id + '`.' + config['env']['v_bqutil_fns_dataset_name'],
        "param_auth_base_views_dataset_name" : bq_project_id + '.' + config['env']['v_auth_base_views_dataset_name'],
        "param_parallon_ra_audit_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_parallon_ra_audit_dataset_name'],
        "param_ops_project_id": config['env']["v_ops_project_id"],
        "param_cm_project_id": config['env']["v_cm_project_id"],
        "param_pub_project_id": config['env']["v_pub_project_id"],
        "param_parallon_cur_project_id": config['env']['v_curated_project_id']
    }
}

with DAG(
        "dag_create_ra_tables_from_ddl",
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        max_active_runs=1,
        max_active_tasks=10,
        template_searchpath='/home/airflow/gcs/'
) as dag:
    start_job = DummyOperator(task_id='start_job')
    end_job = DummyOperator(task_id='end_job')

    folder = Variable.get("var_ra_dataset")
    arr = []

    with TaskGroup(group_id=f'TG-table-{folder}') as folder_grp:
        blob_list = getsqlfilelist('dags/edwra/sql/ddl/' + folder + '/')

        for blob in blob_list:
            if blob.endswith(".sql"):
                dataset = blob.split('/')[3]
                file = blob.split('/')[5].split('.')[0]
                run_sql_ddl = BigQueryOperator(
                    gcp_conn_id=config['env']['v_curated_conn_id'],
                    sql=blob,
                    use_legacy_sql=False,
                    retries=0,
                    task_id=f'run_bqsql_create_table_{dataset}_{file}'
                )
        [run_sql_ddl]

    start_job >> folder_grp >> end_job