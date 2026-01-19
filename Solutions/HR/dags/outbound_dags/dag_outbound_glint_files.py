from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from airflow.operators.python_operator import PythonOperator

from google.cloud import storage

from datetime import datetime, timedelta
import pendulum
import os
import sys
import logging
from airflow.utils.task_group import TaskGroup

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
tgt_folder = config['env']['v_tgtfilesdir']

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

def create_dag( dag_id, schedule, tags, dependency):
    if schedule == "None":
        schedule = None
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args, 
        schedule_interval=schedule,
        catchup=False, 
        max_active_runs=1, 
        template_searchpath='/home/airflow/gcs/dags/sql/',
        tags=tags
    )

    with dag:
        
        #define tasks 
        start_job = DummyOperator(task_id='start_dag')
        end_job = DummyOperator(task_id='end_dag')

        for item in dependency:
            table = item["table"]
            has_sensor = item["has_sensor"]
            sql1 = item["sql"]
            sql= open(f'/home/airflow/gcs/dags/sql/dml/outbound/{sql1}').read()
            source_query = sql
            
            target_file_name_prefix = item["target_file_name_prefix"]
            target_file_extension = item["target_file_extension"]
            delimiter = item["delimiter"]
            quote_char = item["quote_char"]
            is_prefixed_file_name = item["has_prefixed_file_name"]
            encoding_scheme = item["encoding_scheme"]

            #define tasks 

            with TaskGroup(group_id=f'TG-table-{table}') as table_grp:
                table_task = DummyOperator(task_id=f'T-table-{table}') 

                outbound_file = PythonOperator(
                    task_id='outbound_file',
                    provide_context=True,
                    python_callable=cu.staging_into_outbound_file,
                    op_kwargs={
                        'sourcesysname': source_system,
                        'target_file_name_prefix': target_file_name_prefix,
                        'target_file_extension': target_file_extension,
                        'delimiter': delimiter,
                        'quote_char': quote_char,
                        'is_prefixed_file_name': is_prefixed_file_name,
                        'encoding_scheme': encoding_scheme,
                        'source_query': source_query
                    }
                )
                table_task>>outbound_file
            if has_sensor == 'Yes':
                for sensor in item["sensor"]:
                    ext_dag_id = sensor["dag_id"]
                    ext_task_id = sensor["task_id"]
                    schedule = sensor["schedule"]
                    with TaskGroup(group_id=f'TG-sensor-{table}') as sensor_grp:
                        check_task_completion= ExternalTaskSensor(
                            task_id=f"Check_status_{ext_task_id}",
                            external_dag_id=ext_dag_id,
                            external_task_id=ext_task_id,
                            timeout=600,
                            execution_date_fn=cu.get_execution_date,
                            params={"schedule":schedule},
                            allowed_states=["success"],
                            failed_states=["failed", "skipped"],
                            mode="reschedule"
                        )
                        [check_task_completion]
                start_job >> sensor_grp >> table_grp >> end_job
            else:
                start_job >> table_grp >> end_job
    return dag


config_glint = cu.call_config_yaml("hrg_glint_outbound_dependency.yaml","hca_hrg_glint_outbound_dependency")
for outbound in config_glint['outbound']:
    frequency = outbound["frequency"]
    schedule = outbound["schedule"]
    source_system = outbound["source_system"]
    type = outbound["type"]
    if schedule == "None":
        interval_range =  "None"
    else:
        time = outbound["schedule"].split(" ")
        interval_range = time[1].zfill(2) + "."  + time[0].zfill(2)
    dependency = outbound["dependency"]
    tags = [source_system]
    if frequency == "None":
        dag_id = f'dag_outbound_' + source_system + '_' + type + '_adhoc'
        tags.extend(['adhoc'])
    else:
        dag_id = f'dag_outbound_' + source_system + '_' + type + '_' + frequency + '_' + interval_range
        tags.extend([frequency])
    additional_tags = outbound["custom_tags"]
    tags.extend(additional_tags.split(','))
    globals()[dag_id] = create_dag(dag_id, schedule, tags, dependency)