from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy import  DummyOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
import re
import logging

from datetime import datetime, timedelta
import pendulum
import os
import sys

script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, '..', 'utils')
sys.path.append(util_dir)
# import common_utilities after setting the path for utils
import common_utilities as cu

current_timezone = pendulum.timezone("US/Central")

config = cu.call_config_yaml("hrg_config.yaml","hca_hrg_default_vars")

def get_execution_date(execution_date, **kwargs):  
    print("Execution Date - ",execution_date)  
    exec_day = int(execution_date.strftime('%w'))
    print("Execution Day - ",exec_day)
    schedule = kwargs["params"]["schedule"]
    print("Sensor Schedule - ", schedule)
    special_check = kwargs["params"]["special_check"]
    print("Special Check - ", special_check)
    schedule_day = schedule.split()[4]
    if special_check == "noweekend" and exec_day in [0,5,6]:
        if exec_day in [5,6]:
            schedule_day = 4
        else:
            schedule_day = 5
    else:
        if re.search(",",schedule_day):
            schedule_day=schedule_day.split(",")
            if str(exec_day) in schedule_day:
                schedule_day=exec_day
                print("Found")
            else:
                logging.info("Dependant DAG is not scheduled to run today")
                print("Error")
                return
    if schedule_day == "*":
        poke_date = execution_date
        print("Execution Date ", execution_date)
    elif exec_day < int(schedule_day):
        date_diff = 7-int(schedule_day)+exec_day
        print("Date Diff ", date_diff)
        print("Execution Date ", execution_date - timedelta(days=date_diff))
        poke_date = execution_date - timedelta(days=date_diff)        
    elif exec_day >= int(schedule_day):
        date_diff = exec_day-int(schedule_day)
        print("Date Diff ", date_diff)
        print("Execution Date ", execution_date - timedelta(days=date_diff))
        poke_date = execution_date - timedelta(days=date_diff)
    year = int(poke_date.year)
    month = int(poke_date.month)
    date = int(poke_date.day)
    min = int(schedule.split()[0])
    hour = int(schedule.split()[1])
    poke_date = pendulum.datetime(year,month,date,hour,min,0,tz=current_timezone)
    print(poke_date)
    return poke_date

default_args = {
    'owner': 'hca_hrg_atos',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2022, 12, 4, tz=current_timezone),
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
        "param_hr_base_views_dataset_name": config['env']['v_curated_project_id'] + 
                                    '.' + config['env']['v_hr_base_views_dataset_name'],
        "param_hr_views_dataset_name": config['env']['v_curated_project_id'] + 
                                    '.' + config['env']['v_hr_views_dataset_name'],
        "param_pub_views_dataset_name": config['env']['v_curated_project_id'] + 
                                    '.' + config['env']['v_pub_views_dataset_name'],
        }
    }


def create_dag( dag_id, schedule, tags, dependency, source_system):
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
   
        start_job = DummyOperator(task_id='start_job')
        end_job = DummyOperator(task_id='end_job')
        for item in dependency:
            table = item["table"]
            has_sensor = item["has_sensor"]

            with TaskGroup(group_id=f'TG-table-{table}') as table_grp:
                table_task = DummyOperator(task_id=f'T-table-{table}') 
                sql_arr=[]
                #task for calling audit function
                task_id_core = ''
                task_id_core = f'TG-table-{table}.' + f'T-table-{table}'
                update_audit_table = PythonOperator(
                                task_id = f'audit_table_{table}',
                                python_callable = cu.executevalidationsqls,
                                provide_context = True,
                                op_kwargs = {
                                    'bq_table': table,
                                    'task_id' : task_id_core,
                                    'dag_id' : dag_id,
                                    'source' : source_system
                                    
                                } 
                       )                                
                for idx,sql in enumerate(item["sql"]):
                        update_bq_table = BigQueryOperator(
                            gcp_conn_id=config['env']['v_curated_conn_id'],
                            sql = f'dml/integration/incremental/{source_system}/{sql["name"]}',
                            use_legacy_sql=False, 
                            retries=0, 
                            task_id = f'run_{sql["name"]}'
                        )
                        if idx == 0:
                            table_task >>update_bq_table
                        else:
                            sql_arr[-1] >> update_bq_table
                        sql_arr.append(update_bq_table)
                sql_arr[len(sql_arr)-1] >> update_audit_table #adding audit task after the sql runs

            if has_sensor == 'Yes':
                with TaskGroup(group_id=f'TG-sensor-{table}') as sensor_grp:
                    for sensor in item["sensor"]:
                        ext_dag_id = sensor["dag_id"]
                        ext_task_id = sensor["task_id"]
                        schedule = sensor["schedule"]
                        special_check = sensor["special_check"]
                        # with TaskGroup(group_id=f'TG-sensor-{table}-{ext_dag_id.replace(".","_")}-{ext_task_id.replace(".","_")}') as sensor_grp:
                        check_task_completion= ExternalTaskSensor(
                            task_id=f"Check_status_{ext_task_id}",
                            external_dag_id=ext_dag_id,
                            external_task_id=ext_task_id,
                            timeout=21600,
                            execution_date_fn=get_execution_date,
                            params={"schedule":schedule,"special_check":special_check},
                            allowed_states=["success"],
                            failed_states=["failed", "skipped"],
                            mode="reschedule"
                        )
                        [check_task_completion]
                start_job >> sensor_grp >> table_grp >> end_job
            else:
                start_job >> table_grp >> end_job
    return dag

config_enwisen = cu.call_config_yaml("hrg_enwisen_integrate_dependency.yaml","hca_hrg_enwisen_integrate_dependency")
for integrate in config_enwisen['integrate']:
    frequency = integrate["frequency"]
    schedule = integrate["schedule"]
    source_system = integrate["source_system"]
    type = integrate["type"]

    if schedule == "None":
        interval_range = "None"
    else:
        time = integrate["schedule"].split(" ")
        interval_range = time[1].zfill(2) + "."  + time[0].zfill(2)

    dependency = integrate["dependency"]
    
    tags = [source_system]
    if frequency == "None":
        dag_id = f'dag_integrate_' + source_system + '_' + type + '_adhoc'
        tags.extend(['adhoc'])
    else:
        dag_id = f'dag_integrate_' + source_system + '_' + type + '_' + frequency + '_' + interval_range
        tags.extend([frequency])
    additional_tags = integrate["custom_tags"]
    tags.extend(additional_tags.split(','))

    globals()[dag_id] = create_dag(dag_id, schedule, tags, dependency, source_system)