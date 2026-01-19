from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import  DummyOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatusSensor
from airflow.providers.google.cloud.hooks.dataflow import DataflowJobStatus
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
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
util_dir = os.path.join(script_dir, '..', 'utils')
sys.path.append(util_dir)
current_timezone = pendulum.timezone("US/Central")
import common_utilities as cu

config_name = "ra_config.yaml"
config = cu.call_config_yaml(config_name, "hca_ra_default_vars")
lob = config['env']['v_lob']
bq_project_id = config['env']['v_curated_project_id']
gcs_project_id = config['env']['v_landing_project_id']
bq_staging_dataset = config['env']['v_parallon_ra_core_dataset_name']
stage_dataset = config['env']['v_parallon_ra_core_dataset_name']
gcs_bucket = config['env']['v_data_bucket_name']
src_folder = config['env']['v_srcfilesdir']


default_args = {
    'owner': 'hca_ra_atos',
    'depends_on_past': False,
    'email': config['env']['v_failure_email_list'],
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=240),
    # params to substitute in sql files
    'params': {
        "param_parallon_ra_stage_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_parallon_ra_stage_dataset_name'],
        "param_parallon_ra_core_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_parallon_ra_core_dataset_name'],
        "param_parallon_ra_base_views_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_parallon_ra_base_views_dataset_name'],
        "param_parallon_ra_views_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_parallon_ra_views_dataset_name'],
        "param_parallon_ra_audit_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_parallon_ra_audit_dataset_name'],
        "param_parallon_ra_bqutil_fns_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_bqutil_fns_dataset_name'],
        "param_auth_base_views_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_auth_base_views_dataset_name'],
        "param_parallon_cur_project_id": config['env']['v_curated_project_id']
        }
    }


def create_dag(dag_id, schedule, start_date, dependency, source_system, frequency, tags, done_file):
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args, 
        start_date=eval(start_date),
        schedule_interval=schedule, 
        catchup=False, 
        max_active_runs=1, 
        template_searchpath='/home/airflow/gcs/dags/edwra/sql/',
        tags=[f"{source_system}", f"{frequency}", "ra"]
    )

    with dag:
   
        start_job = DummyOperator(task_id='start_job')
        end_job = DummyOperator(task_id='end_job')
        table_grp_arr = []

        if done_file != "None":
            try:
                if frequency == "daily":
                    now = pendulum.now(current_timezone).strftime("%Y%m%d")
                    prev = (pendulum.now(current_timezone).subtract(days=7)).strftime("%Y%m%d")
                elif frequency == "weekly":
                    now = pendulum.yesterday(current_timezone).strftime("%Y%m%d")
                    prev = (pendulum.yesterday(current_timezone).subtract(days=7)).strftime("%Y%m%d")
                elif frequency == "monthly":
                    now = pendulum.now(current_timezone).strftime("%Y%m%d")
                    prev = (pendulum.now(current_timezone).subtract(days=30)).strftime("%Y%m%d")
            except:
                logging.info(
                    "================={} is not a valid frequency(daily/weekly/monthly)=================".format(frequency))
            
            # prev = (pendulum.now(current_timezone).subtract(days=2)).strftime("%Y%m%d")
            done_file_to_delete = done_file.replace('YYYYMMDD', prev)
            done_file = done_file.replace('YYYYMMDD', now)
            file_sensor = GCSObjectsWithPrefixExistenceSensor(
                bucket=gcs_bucket,
                prefix=src_folder + source_system + "/" + done_file,
                timeout=10800,
                mode="reschedule",
                task_id="check_pa_done_file_exists"
            )
    
            delete_old_done_files = PythonOperator(
                task_id='delete_old_done_files',
                python_callable=cu.delete_files_with_filename,
                op_kwargs={
                    'gcs_bucket_name': gcs_bucket,
                    'gcs_folder': src_folder + source_system,
                    'gcs_project_id': gcs_project_id,
                    'target_filename': done_file_to_delete
                }
                )

        for ind, item in enumerate(dependency):
            table = item["table"]
            has_sensor = item["has_sensor"]
            has_file_export = item["has_file_export"]            
            with TaskGroup(group_id=f'TG-table-{table}') as table_grp:
                sql_arr = []
                # task for calling audit function
                task_id_core = f'TG-table-{table}.' + f'run_{item["sql"][0]["name"]}'
                update_audit_table = PythonOperator(
                                task_id=f'audit_table_{table}',
                                python_callable=cu.executevalidationsqls,
                                provide_context=True,
                                op_kwargs={
                                    'bq_table': table,
                                    'task_id': task_id_core,
                                    'dag_id': dag_id,
                                    'source': source_system,
                                    'config': config,
                                    'replacements': default_args['params']
                                } 
                       )

                for idx, sql in enumerate(item["sql"]):
                    update_bq_table = BigQueryOperator(
                        gcp_conn_id=config['env']['v_curated_conn_id'],
                        # sql = f'dml/integration/incremental/{lob}/{source_system}/{sql["name"]}',
                        sql=f'dml/integration/incremental/{sql["name"]}',
                        use_legacy_sql=False,
                        retries=0,
                        task_id=f'run_{sql["name"]}',
                        dag=dag
                    )
                    if idx == 0:
                        #table_task >> update_bq_table
                        [update_bq_table]
                    else:
                        sql_arr[-1] >> update_bq_table
                    sql_arr.append(update_bq_table)
                sql_arr[len(sql_arr)-1] >> update_audit_table  # adding audit task after the sql runs
                # sql_arr[len(sql_arr)-1] #adding audit task after the sql runs
        
            if has_sensor == 'Yes':
                with TaskGroup(group_id=f'TG-sensor-{source_system}') as sensor_grp:
                    for sensor in item["sensor"]:
                        ext_dag_id = sensor["dag_id"]
                        ext_task_id = sensor["task_id"]
                        sensor_schedule = sensor["schedule"]
                        check_task_completion = ExternalTaskSensor(
                            task_id=f"Check_status_{ext_dag_id}",
                            external_dag_id=ext_dag_id,
                            external_task_id=ext_task_id,
                            timeout=10800,
                            execution_date_fn=cu.get_execution_date,
                            params={"schedule": sensor_schedule},
                            allowed_states=["success"],
                            failed_states=["failed", "skipped"],
                            mode="reschedule"
                        )
                        [check_task_completion]

            if has_file_export == 'Yes' : 
                source_files = []
                for idx, source_file_name in enumerate(item["source_file_name"]):
                    source_files.append(str(source_file_name))
                source_file_str = ' '.join(source_files)
                
                dest_files = [] 
                for idx, dest_file_name in enumerate(item["destination_file_name"]):
                    dest_files.append(str(dest_file_name))
                dest_files_str = ' '.join(dest_files)
                
                file_export_df_job = BashOperator(
                    task_id="file_export_to_server",
                    dag = dag,
                    params = {'source_files': source_file_str, 'dest_files': dest_files_str},
                    bash_command="python /home/airflow/gcs/dags/edwra/scripts/file_export_df.py --source_files {} --dest_files {} --src_sys_config_file={}".format(source_file_str, dest_files_str, config_name),
                )

                # Sensor that checks Dataflow job status every 1 minute
                # There are options to continuously check for status but this way clears up slots
                wait_for_python_job_async_done = DataflowJobStatusSensor(
                    task_id="wait_for_python_job_async_done",
                    job_id=f"{{{{ task_instance.xcom_pull(task_ids= 'file_export_to_server')}}}}",
                    expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
                    mode="reschedule",
                    poke_interval= 30,
                    location='us-east4',
                )
            if has_file_export == 'Yes':
                if done_file == "None":
                    if ind == 0 and has_sensor == 'Yes':
                        start_job >> sensor_grp >> table_grp >> file_export_df_job >> wait_for_python_job_async_done >> end_job
                    elif ind == 0 and has_sensor == 'No':
                        start_job >> table_grp >> file_export_df_job >> wait_for_python_job_async_done >> end_job
                    else:
                        table_grp_arr[-1] >> table_grp >> file_export_df_job >> wait_for_python_job_async_done
                    table_grp_arr.append(table_grp)
                else:
                    if ind == 0 and has_sensor == 'Yes':
                        start_job >> file_sensor >> sensor_grp >> table_grp >> delete_old_done_files >> file_export_df_job >> wait_for_python_job_async_done >> end_job
                    elif ind == 0 and has_sensor == 'No':
                        start_job >> file_sensor >> table_grp >> delete_old_done_files >> file_export_df_job >> wait_for_python_job_async_done >> end_job
                    else:
                        table_grp_arr[-1] >> file_export_df_job >> wait_for_python_job_async_done >> table_grp
                    table_grp_arr.append(table_grp)
            else:
                if done_file == "None":
                    if ind == 0 and has_sensor == 'Yes':
                        start_job >> sensor_grp >> table_grp >>  end_job
                    elif ind == 0 and has_sensor == 'No':
                        start_job >> table_grp >> end_job
                    else:
                        table_grp_arr[-1] >> table_grp
                    table_grp_arr.append(table_grp)
                else:
                    if ind == 0 and has_sensor == 'Yes':
                        start_job >> file_sensor >> sensor_grp >> table_grp >> delete_old_done_files >> end_job
                    elif ind == 0 and has_sensor == 'No':
                        start_job >> file_sensor >> table_grp >> delete_old_done_files >> end_job
                    else:
                        table_grp_arr[-1] >> table_grp
                    table_grp_arr.append(table_grp)
               
    return dag


config_paraedw_integrate = cu.call_config_yaml("ra_paraedw_integrate_dependency.yaml", "ra_paraedw_integrate_dependency")

for integrate in config_paraedw_integrate['integrate']:
    frequency = integrate["frequency"]
    schedule = integrate["schedule"]
    start_date = integrate["start_date"]
    source_system = integrate["source_system"]
    type = integrate["type"]
    dependency = integrate["dependency"]
    done_file = integrate["done_file"]

    if schedule == "None":
        interval_range = "None"
    else:
        time = integrate["schedule"].split(" ")
        interval_range = time[1].zfill(2) + "." + time[0].zfill(2)
        interval_range = interval_range.replace("/", "-")

    tags = [source_system]

    if frequency == "None":
        dag_id = f'dag_integrate_ra_{source_system}_{type}_{frequency}_adhoc'
        tags.extend(['adhoc', 'ra'])
    else:
        dag_id = f'dag_integrate_ra_{source_system}_{type}_{frequency}_{interval_range}'
        tags.extend([frequency, 'ra'])

    globals()[dag_id] = create_dag(dag_id, schedule, start_date, dependency, source_system, frequency, tags, done_file)
