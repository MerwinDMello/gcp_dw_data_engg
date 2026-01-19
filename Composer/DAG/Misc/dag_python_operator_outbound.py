from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator

from google.cloud import storage

from datetime import datetime, timedelta
import pendulum
import re
import os
import sys
import logging
import pandas as pd

from google.cloud import bigquery

script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, '..', 'utils')
sys.path.append(util_dir)
# import common_utilities after setting the path for utils
import common_utilities as cu

logging.basicConfig(level=logging.INFO)

config = cu.call_config_yaml("hrg_config.yaml", "hca_hrg_default_vars")
# get project variables from config file
bq_project_id = config['env']['v_curated_project_id']
bq_staging_dataset = config['env']['v_hr_stage_dataset_name']
gcs_bucket = config['env']['v_data_bucket_name']
tgt_folder = "edwhrdata/outboundfiles/"
# src_folder = config['env']['v_srcfilesdir']
# arc_folder = config['env']['v_archivedir']

current_timezone = "US/Central"

default_args = {
    'owner': 'hca_hrg_atos',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2022, 12, 9, tz=current_timezone),
    # 'email': config['env']['v_failure_email_list'],
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
        "param_hr_base_views_dataset_name": config['env']['v_curated_project_id'] + 
                                    '.' + config['env']['v_hr_base_views_dataset_name'],
        "param_hr_views_dataset_name": config['env']['v_curated_project_id'] + 
                                    '.' + config['env']['v_hr_views_dataset_name'],
        "param_pub_views_dataset_name": config['env']['v_curated_project_id'] + 
                                    '.' + config['env']['v_pub_views_dataset_name'],
        }
}

def staging_into_outbound_file(**kwargs):
    sourcesysname = kwargs['sourcesysname']
    target_file_name_prefix = kwargs['target_file_name_prefix']
    target_file_extension = kwargs['target_file_extension']
    src_bq_table = kwargs['src_bq_table']
    delimiter = kwargs['delimiter']
    quote_char = kwargs['quote_char']
    is_prefixed_file_name = kwargs['is_prefixed_file_name']
    encoding_scheme = kwargs['encoding_scheme']
    bq_staging_dataset = kwargs['bq_staging_dataset']

    source_query = "Select * from " + bq_project_id + "." + bq_staging_dataset + "." + src_bq_table
    bqclient = bigquery.Client(bq_project_id)
    bq_query = bqclient.query(source_query)
    # df = bq_query.to_dataframe()
    result = bq_query.result()
    print(result)
    if is_prefixed_file_name == 'Yes':
        target_file_name = target_file_name_prefix + "_" + \
        datetime.now(pendulum.timezone(current_timezone)).strftime('%Y%m%d') + target_file_extension
    else:
        target_file_name = target_file_name_prefix + target_file_extension

    storage_client = storage.Client(bq_project_id)
    bucket = storage_client.bucket(gcs_bucket)
    tgtfilefullpath = '{}{}/{}'.format(tgt_folder, sourcesysname, target_file_name)
    blob = bucket.blob(tgtfilefullpath)
    blob.upload_from_string(df.to_csv(sep=delimiter, quotechar=quote_char, encoding=encoding_scheme, quoting=1, index=False))

    return "File has been extracted from Staging"

with DAG(
    "dag_outbound_glint_files_daily", 
    default_args=default_args, 
    schedule_interval="0 6 * * *",
    catchup=False, 
    max_active_runs=1, 
    template_searchpath='/home/airflow/gcs/dags/sql/',
    tags=["enwisen"]
) as dag:

    sourcesysname = 'glint'
    target_file_name_prefix = 'Glint_Question'
    target_file_extension = '.txt'
    src_bq_table = 'glint_question'
    delimiter = '|'
    quote_char = '"'
    is_prefixed_file_name = 'Yes'
    encoding_scheme = 'utf-8'
    # source_file_path = intermediate_path + file_name + file_extension
    # archive_file_path = intermediate_path + file_name + '_' + \
    # datetime.now(pendulum.timezone(current_timezone)).strftime('%Y%m%d') + file_extension

    #define tasks 
    start_job = DummyOperator(task_id='start_dag')
    end_job = DummyOperator(task_id='end_dag')

    outbound_file = PythonOperator(
        task_id='outbound_file',
        provide_context=True,
        python_callable=staging_into_outbound_file,
        op_kwargs={
            'sourcesysname': sourcesysname,
            'target_file_name_prefix': target_file_name_prefix,
            'target_file_extension': target_file_extension,
            'bq_staging_dataset': bq_staging_dataset,
            'src_bq_table': src_bq_table,
            'delimiter': delimiter,
            'quote_char': quote_char,
            'is_prefixed_file_name': is_prefixed_file_name,
            'encoding_scheme': encoding_scheme
        }
    )

#setting dag dependency
start_job >> outbound_file >> end_job