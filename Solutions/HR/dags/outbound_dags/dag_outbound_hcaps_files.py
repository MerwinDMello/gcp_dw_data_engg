from airflow import DAG
from airflow.models import Variable

from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GCSToBigQueryOperator
from airflow.contrib.operators.gcs_to_gcs import GCSToGCSOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import  DummyOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain
from google.cloud import bigquery

import json 
import yaml
from datetime import datetime, timedelta
import os,sys
import pendulum
import pandas as pd

script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, '..', 'utils')
sys.path.append(util_dir)
# import common_utilities after setting the path for utils
import common_utilities as cu

config = cu.call_config_yaml("hrg_config.yaml","hca_hrg_default_vars")
bq_project_id = config['env']['v_curated_project_id']
gcs_bucket = config['env']['v_data_bucket_name']
tgt_folder = config['env']['v_tgtfilesdir']
bucket_name = f'{gcs_bucket}/{tgt_folder}hcaps'

bqclient = bigquery.Client(bq_project_id)

timezone = pendulum.timezone("US/Central")
default_args = {
    'owner': 'hca_hrg_atos',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2022, 12, 6, tz=timezone),
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
    
def get_execution_date(execution_date, **kwargs):  
    print(execution_date)  
    exec_day = int(execution_date.strftime('%w'))
    print(exec_day)
    schedule = kwargs["params"]["schedule"]
    print(schedule)
    for item in schedule:
        if len(schedule) == 1:
            schedule = item
        else:
            now = pendulum.now(timezone)
            if now.month >= int(item.split(" ")[3]):
                schedule = item
                break
    print(schedule)
    schedule_day = schedule.split()[4]
    print(schedule_day)
    if schedule_day == "*":
        poke_date = execution_date
    elif exec_day == int(schedule_day):
        date_diff = 0
        poke_date = execution_date - timedelta(days=date_diff)
        print("Diff : ", date_diff)
    elif exec_day < int(schedule_day):
        date_diff = 7-int(schedule_day)+exec_day
        poke_date = execution_date - timedelta(days=date_diff)
        print("Diff : ", date_diff)
    elif exec_day > int(schedule_day):
        date_diff = exec_day-int(schedule_day)
        poke_date = execution_date - timedelta(days=date_diff)
        print("Diff : ", date_diff)

    print("Poke Date : ", poke_date)
    year = int(poke_date.year)
    month = int(poke_date.month)
    date = int(poke_date.day)
    min = int(schedule.split()[0])
    hour = int(schedule.split()[1])
    poke_date = pendulum.datetime(year,month,date,hour,min,0,tz=timezone)
    print(poke_date)
    return poke_date 

def export_data(table,file_name):
    destination_uri = "gs://{}/{}*.csv".format(bucket_name, file_name)
    dataset = config['env']['v_hr_stage_dataset_name']
    dataset_ref = bigquery.DatasetReference(bq_project_id, dataset)
    table_ref = dataset_ref.table(table) 

    extract_job = bqclient.extract_table(
        table_ref,
        destination_uri,
        # Location must match that of the source table.
        location="US",
    )  # API request
    extract_job.result()  # Waits for job to complete.
    print(
        "Exported {}:{}.{} to {}".format(bq_project_id, dataset, table, destination_uri)
    )



def create_dag( dag_id, schedule, dependency):
    if schedule == "None":
        schedule = None
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args, 
        schedule_interval=schedule,
        catchup=False, 
        max_active_runs=1, 
        template_searchpath='/home/airflow/gcs/dags/sql/',
        tags=["hcaps"]
    )
    with dag:

        #define tasks 
        start_job = DummyOperator(task_id='start_dag')
        end_job = DummyOperator(task_id='end_dag')

        for item in dependency:
            sourcesysname = "hcaps"
            target = item["target"].get("name")
            source_query = open(f'/home/airflow/gcs/dags/sql/dml/outbound/{item["source"].get("sql")}').read()
            has_sensor = item["source"].get("sensor")

            with TaskGroup(group_id=f'TG-table-{target}') as table_grp:

                outbound_file = PythonOperator(
                    task_id = f'outbound_file_{target}',
                    python_callable = export_data,
                    op_kwargs = {
                          'table' : item["source"].get("name"),
                          'file_name' : item["target"].get("name")
                    #     # 'source_query' : source_query, 
                    #     # 'sourcesysname' : sourcesysname, 
                    #     # 'is_prefixed_file_name' : 'Yes',
                    #     # 'target_file_name_prefix' : item["target"].get("name"),
                    #     # 'target_file_extension' : "." + item["target"].get("extension"),
                    #     # 'delimiter' : item["target"].get("delimiter"), 
                    #     # 'quote_char' : item["target"].get("quote_char"), 
                    #     # 'encoding_scheme' : item["target"].get("encoding_scheme")
                    }
                )

                if has_sensor == 'Yes':
                    for sensor in item["sensor"]:
                        ext_dag_id = sensor.get("dag_id")
                        ext_task_id = sensor.get("task_id")
                        schedule = sensor.get("schedule")
                        with TaskGroup(group_id=f'TG-sensor-{target}') as sensor_grp:
                            check_task_completion= ExternalTaskSensor(
                                task_id=f"Check_status_{ext_task_id}",
                                external_dag_id=ext_dag_id,
                                external_task_id=ext_task_id,
                                timeout=600,
                                execution_date_fn=get_execution_date,
                                params={"schedule":schedule},
                                allowed_states=["success"],
                                failed_states=["failed", "skipped"],
                                mode="reschedule"
                            )
                            [check_task_completion]
                    [sensor_grp] >> outbound_file    
                else: 
                    outbound_file

                
        #setting dag dependency
            start_job >> [table_grp] >> end_job
        return dag

config_hcaps = cu.call_config_yaml("hrg_hcaps_outbound_dependency.yaml","hca_hrg_hcaps_outbound_dependency")
for outbound in config_hcaps['outbound']:
    frequency = outbound["frequency"]
    schedule = outbound["schedule"]
    source_system = outbound["source_system"]
    type = outbound["type"]
    for item in schedule:
        if len(schedule) == 1:
            if item == "None":
                interval_range =  "None"
                schedule = None
            else:
                schedule = item
                time = item.split(" ")
                interval_range = time[1].zfill(2) + "."  + time[0].zfill(2)
        else:
            now = pendulum.now(timezone)
            if now.month <= int(item.split(" ")[3]):
                schedule = item
                time = item.split(" ")
                interval_range = time[1].zfill(2) + "."  + time[0].zfill(2)
                break
    dependency = outbound["dependency"]
    dag_id = f'dag_outbound_'+source_system+'_'+type+'_' + frequency + '_' + interval_range
    globals()[dag_id] = create_dag(dag_id, schedule, dependency)