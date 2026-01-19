from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy import  DummyOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from airflow.operators.python import PythonOperator
import json 
import yaml
from datetime import datetime, timedelta
import os,sys
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain
from airflow.models import DagRun
import pendulum
timezone = pendulum.timezone("US/Central")


script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, '..', 'utils')
sys.path.append(util_dir)
import common_utilities as cu

config = cu.call_config_yaml("hrg_config.yaml","hca_hrg_default_vars")

default_args = {
    'owner': 'hca_hrg_atos',
    'depends_on_past': False,
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
                                    '.' + config['env']['v_hr_stage_dataset_name'],

        "param_hr_core_dataset_name": config['env']['v_curated_project_id'] + 
                                    '.' + config['env']['v_hr_core_dataset_name'],

        "param_hr_base_views_dataset_name": config['env']['v_curated_project_id'] + 
                                    '.' + config['env']['v_hr_base_views_dataset_name'],

        "param_dim_base_views_dataset_name": config['env']['v_curated_project_id'] + 
                                    '.' + config['env']['v_dim_base_views_dataset_name'],

        "param_hr_views_dataset_name": config['env']['v_curated_project_id'] + 
                                    '.' + config['env']['v_hr_views_dataset_name'],

        "param_pub_views_dataset_name": config['env']['v_curated_project_id'] + 
                                    '.' + config['env']['v_pub_views_dataset_name'], 

        "param_dim_core_dataset_name": config['env']['v_curated_project_id'] + 
                                    '.' + config['env']['v_dim_core_dataset_name'],  

        "param_fs_base_views_dataset_name": config['env']['v_curated_project_id'] + 
                                    '.' + config['env']['v_fs_base_views_dataset_name'],

        "param_fs_core_dataset_name": config['env']['v_curated_project_id'] + 
                                    '.' + config['env']['v_fs_core_dataset_name']                                                                                                                                        
        }
    }

def get_execution_date(execution_date, **kwargs):  
# Function used in execution_date_fn of ExternalTaskSensor task
    print("==== In get_exceution_date with params ====")
    print(execution_date, kwargs)
    exec_day = int(execution_date.strftime('%w'))
    print(exec_day)
    schedule = kwargs["params"]["schedule"]
    print(schedule)
    for item in schedule:
        if len(schedule) == 1:
            schedule = item
        else:
            now = pendulum.now(timezone)
            if now.month <= int(item.split(" ")[3]):
                schedule = item
                break
    print(schedule)
    schedule_day = schedule.split()[4]
    if schedule_day == "*":
        poke_date = execution_date
    elif exec_day <= int(schedule_day):
        date_diff = 7-int(schedule_day)+exec_day
        print(date_diff)
        print(execution_date - timedelta(days=date_diff))
        poke_date = execution_date - timedelta(days=date_diff)
        
    elif exec_day > int(schedule_day):
        date_diff = exec_day-int(schedule_day)
        print(execution_date - timedelta(days=date_diff))
        poke_date = execution_date - timedelta(days=date_diff)

    year = int(poke_date.year)
    month = int(poke_date.month)
    date = int(poke_date.day)
    min = int(schedule.split()[0])
    hour = int(schedule.split()[1])
    poke_date = pendulum.datetime(year,month,date,hour,min,0,tz=timezone)
    print(poke_date)
    return poke_date
def create_dag( dag_id, schedule, dependency,frequency,start_date, source_system):
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args, 
        start_date = eval(start_date),
        schedule_interval=schedule, 
        catchup=False, 
        max_active_runs=1, 
        template_searchpath='/home/airflow/gcs/dags/sql/',
        tags=[f"{source_system}", f"{frequency}"]
    )
    with dag:
   
        start_job = DummyOperator(task_id='start_job')
        end_job = DummyOperator(task_id='end_job')
        table_grp_arr = []
        for ind, item in enumerate(dependency):
            table = item["table"]
            has_sensor = item["has_sensor"]            
            with TaskGroup(group_id=f'TG-table-{table}') as table_grp:
                table_task = DummyOperator(task_id=f'T-table-{table}') 
                sql_arr=[]                                
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
                            table_task >> update_bq_table
                            #[update_bq_table]
                        else:
                            sql_arr[-1] >> update_bq_table
                        sql_arr.append(update_bq_table) 
                sql_arr[len(sql_arr)-1] >> update_audit_table #adding audit task after the sql runs

            
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
                            execution_date_fn=get_execution_date,
                            params={"schedule":schedule},
                            allowed_states=["success"],
                            failed_states=["failed", "skipped"],
                            mode="reschedule"
                        )
                        [check_task_completion]                            
            
            

            if ind == 0:
                start_job >> table_grp
            else:
                table_grp_arr[-1] >> table_grp
            table_grp_arr.append(table_grp)
        
        table_grp >> end_job
                        
               
    return dag

config_galen = cu.call_config_yaml("hrg_galen_integrate_dependency.yaml","hca_hrg_galen_integrate_dependency")
for integrate in config_galen['integrate']:
    frequency = integrate["frequency"]
    schedule = integrate["schedule"]
    start_date = integrate["start_date"]
    source_system = integrate["source_system"]
    type = integrate["type"]

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

    dependency = integrate["dependency"]
    dag_id = f'dag_integrate_'+source_system+'_'+type+'_' + frequency + '_' +interval_range
    globals()[dag_id] = create_dag(dag_id, schedule, dependency,frequency,start_date, source_system)
