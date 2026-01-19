from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import  DummyOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator
import json 
import yaml
from datetime import datetime, timedelta
import os,sys
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain
from airflow.models import DagRun
import pendulum
import logging
from google.cloud import storage

timezone = pendulum.timezone("US/Central")

script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, '..','utils')
sys.path.append(util_dir)
current_timezone = pendulum.timezone("US/Central")
import common_utilities as cu

lob = "edwcr"
lob = lob.lower().strip()
lob_abbr = lob.replace("edw","")
src_system = "cdm"
src_system = src_system.lower().strip()
config = cu.call_config_yaml(f"{lob_abbr}_config.yaml", f"hca_{lob_abbr}_default_vars")
lob = config['env']['v_lob']
bq_project_id = config['env']['v_curated_project_id']
gcs_project_id = config['env']['v_landing_project_id']
bq_staging_dataset = config['env'][f'v_{lob_abbr}_stage_dataset_name']
gcs_bucket = config['env']['v_data_bucket_name']
src_folder = config['env']['v_srcfilesdir']
arc_folder = config['env']['v_archivedir']
tgt_folder = config['env']['v_tgtfilesdir']
tmp_folder = config['env']['v_tmpobjdir']
schema_folder = tmp_folder + config['env']['v_schemasubdir']

default_args = {
    'owner': f'hca_{lob_abbr}_atos',
    'depends_on_past': False,
    'email': config['env']['v_failure_email_list'],
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=480),
    #params to substitute in sql files
    'params': {
        f"param_{lob_abbr}_stage_dataset_name": config['env']['v_curated_project_id'] + 
                                    '.' + config['env'][f'v_{lob_abbr}_stage_dataset_name'],
        f"param_{lob_abbr}_core_dataset_name": config['env']['v_curated_project_id'] + 
                                    '.' + config['env'][f'v_{lob_abbr}_core_dataset_name'],
        f"param_{lob_abbr}_base_views_dataset_name": config['env']['v_curated_project_id'] + 
                                    '.' + config['env'][f'v_{lob_abbr}_base_views_dataset_name'],
        f"param_{lob_abbr}_views_dataset_name": config['env']['v_curated_project_id'] + 
                                    '.' + config['env'][f'v_{lob_abbr}_views_dataset_name'],                                    
        f"param_{lob_abbr}_audit_dataset_name": config['env']['v_curated_project_id'] + 
                                    '.' + config['env'][f'v_{lob_abbr}_audit_dataset_name'], 
        f"param_{lob_abbr}_procs_dataset_name" : config['env']['v_curated_project_id'] + 
                                    '.' + config['env'][f'v_{lob_abbr}_procs_dataset_name'], 
        f"param_{lob_abbr}_bqutil_fns_dataset_name" :"`"+config['env']['v_curated_project_id'] + 
                                    '.' + config['env'][f'v_bqutil_fns_dataset_name'] + "`",
        f"param_{lob_abbr}_auth_base_views_dataset_name" : config['env']['v_curated_project_id'] + 
                                    '.' + config['env']['v_auth_base_views_dataset_name'],
        # f"param_{lob_abbr}_scri_views_dataset_name" : config['env']['v_clinical_scri_project_id'] + 
        #                             '.' + config['env']['v_scri_views_dataset_name'],
        # f"param_{lob_abbr}_abstraction_views_dataset_name" : config['env']['v_clinical_abstraction_project_id'] + 
        #                             '.' + config['env']['v_abstraction_views_dataset_name'],
        }
    }

def create_dag(dag_id, schedule, start_date, dependency, source_system, frequency, tags, done_file,):
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args, 
        start_date=eval(start_date),
        schedule_interval=schedule,
        is_paused_upon_creation=True,
        catchup=False, 
        max_active_runs=1, 
        template_searchpath='/home/airflow/gcs/dags/edwcr/sql/',
        tags=tags
    )

    with dag:
   
        start_job = DummyOperator(task_id='start_job')
        end_job = DummyOperator(task_id='end_job')
        table_grp_arr = []

        if done_file != "None":
            try:
                if frequency== "daily":
                    now = pendulum.now(current_timezone).strftime("%Y%m%d")
                    prev = (pendulum.now(current_timezone).subtract(days=7)).strftime("%Y%m%d")
                elif frequency== "weekly":
                    now = pendulum.yesterday(current_timezone).strftime("%Y%m%d")
                    prev = (pendulum.yesterday(current_timezone).subtract(days=7)).strftime("%Y%m%d")
                elif frequency== "monthly":
                    now = pendulum.now(current_timezone).strftime("%Y%m%d")
                    prev = (pendulum.now(current_timezone).subtract(days=30)).strftime("%Y%m%d")
            except:
                logging.info(
                    "================={} is not a valid frequency(daily/weekly)=================".format(frequency))
            
            # prev = (pendulum.now(current_timezone).subtract(days=2)).strftime("%Y%m%d")
            done_file_to_delete =  done_file.replace('YYYYMMDD', prev) 
            done_file= done_file.replace('YYYYMMDD', now)  
            file_sensor = GCSObjectsWithPrefixExistenceSensor(
                bucket=gcs_bucket,
                prefix=src_folder + source_system + "/" + done_file,
                timeout=10800,
                mode="reschedule",
                task_id="check_done_file_exists"
            )
    
            delete_old_done_files = PythonOperator(
                task_id='delete_old_done_files',
                python_callable=cu.delete_files_with_filename,
                op_kwargs={
                    'gcs_bucket_name' : gcs_bucket,
                    'gcs_folder' : src_folder + source_system ,
                    'gcs_project_id' : gcs_project_id,
                    'target_filename' : done_file
                }
                )

        for ind, item in enumerate(dependency):
            table = item["table"]
            has_sensor = item["has_sensor"]
            validation_file_list = item["validation"]
            file_list = [f"{file_name['name']}" for file_name in validation_file_list] 
            with TaskGroup(group_id=f'TG-table-{table}') as table_grp:
                sql_arr=[]  
                #task for calling audit function
                task_id_core = ''
                task_id_core = f'TG-table-{table}.' +  f'run_{item["sql"][0]["name"]}'
                update_audit_table = PythonOperator(
                                task_id = f'audit_table_{table}',
                                python_callable = cu.executevalidationsqls,
                                provide_context = True,
                                op_kwargs = {
                                    'bq_table': table,
                                    'task_id' : task_id_core,
                                    'dag_id' : dag_id,
                                    'source' : source_system,
                                    'config' : config,
                                    'replacements' : default_args['params'],
                                    'file_list'     : file_list                                 
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
                            #table_task >> update_bq_table
                            [update_bq_table]
                        else:
                            sql_arr[-1] >> update_bq_table
                        sql_arr.append(update_bq_table) 
                sql_arr[len(sql_arr)-1] >> update_audit_table #adding audit task after the sql runs
        
            if has_sensor == 'Yes':
                with TaskGroup(group_id=f'TG-sensor-{source_system}-{table}') as sensor_grp:
                    for sensor in item["sensor"]:
                        ext_dag_id = sensor["dag_id"]
                        ext_task_id = sensor["task_id"]
                        schedule = sensor["schedule"]
                        if "cycle_age" in sensor:
                            cycle_age = sensor["cycle_age"]
                        else:
                            cycle_age = "current"
                        if "sensor_dag_frequency" in sensor:
                            sensor_dag_frequency = sensor["sensor_dag_frequency"]
                        else:
                            sensor_dag_frequency = "daily"
                        if "hour_interval" in sensor:
                            hour_interval = int(sensor["hour_interval"])
                        else:
                            hour_interval = 0
                        if "day_interval" in sensor:
                            day_interval = sensor["day_interval"]
                        else:
                            day_interval = 0
                        check_task_completion= ExternalTaskSensor(
                            task_id=f"Check_status_{ext_dag_id}",
                            external_dag_id=ext_dag_id,
                            external_task_id=ext_task_id,
                            timeout=10800,
                            execution_date_fn=cu.get_execution_date,
                            params={"schedule":schedule,"frequency":frequency,"cycle_age":cycle_age,
                            "sensor_dag_frequency":sensor_dag_frequency, "hour_interval": hour_interval, "day_interval":day_interval},
                            allowed_states=["success"],
                            failed_states=["failed", "skipped"],
                            mode="reschedule"
                        )
                        [check_task_completion]
            
            if ind == 0:
                if done_file == "None":
                    if has_sensor == 'Yes':
                        start_job >> sensor_grp >> table_grp
                    else:
                        start_job >> table_grp
                else:
                    if has_sensor == 'Yes':
                        start_job >> file_sensor >> sensor_grp >> table_grp
                    else:
                        start_job >> file_sensor >> table_grp
            else:
                if has_sensor == 'Yes':
                    table_grp_arr[-1] >> sensor_grp >> table_grp
                else:
                    table_grp_arr[-1] >> table_grp

            table_grp_arr.append(table_grp)

        if trigger_dag_ids:
            with TaskGroup(group_id=f'TG-trigger_dag') as trigger_dag_grp:
                for trigger_dag_id in trigger_dag_ids:
                        trigger_dag = TriggerDagRunOperator(
                        task_id=f"trigger_dag_{trigger_dag_id}",
                        trigger_dag_id=trigger_dag_id,
                )
            [trigger_dag_grp]

        if trigger_dag_ids:
            if done_file == "None":
                table_grp >> trigger_dag_grp >> end_job
            else:
                table_grp >> delete_old_done_files >> trigger_dag_grp >> end_job
        else:
            if done_file == "None":
                table_grp >> end_job
            else:
                table_grp >> delete_old_done_files >> end_job

    return dag

integrate_config = cu.call_config_yaml(f"{src_system}_integrate_dependency.yaml", f"{src_system}_integrate_dependency")
for integrate in integrate_config['integrate']:
    frequency = integrate["frequency"]
    schedule = integrate["schedule"]
    start_date = integrate["start_date"]
    source_system = integrate["source_system"]
    type = integrate["type"]
    dependency = integrate["dependency"]
    done_file = integrate["done_file"]
    dag_id_suffix = str(integrate["dag_id_suffix"]).zfill(2)
    trigger_dag_ids = integrate["trigger_dag_ids"]

    if schedule == "None":
        schedule = None

    tags = [source_system]

    if frequency == "None":
        dag_id = f'dag_integrate_{source_system}_{type}_adhoc_{dag_id_suffix}'
        tags.extend(['adhoc', lob_abbr])
    else:
        dag_id = f'dag_integrate_{source_system}_{type}_{frequency}_{dag_id_suffix}'
        tags.extend([frequency, lob_abbr])

    globals()[dag_id] = create_dag(dag_id, schedule, start_date, dependency, source_system, frequency, tags, done_file)