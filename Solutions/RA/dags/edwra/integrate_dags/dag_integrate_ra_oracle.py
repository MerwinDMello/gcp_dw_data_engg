from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatusSensor
from airflow.providers.google.cloud.hooks.dataflow import DataflowJobStatus
from datetime import datetime, timedelta, date
import calendar
import os
from airflow.utils.task_group import TaskGroup
import pendulum
import sys

script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, '..', 'utils')
sys.path.append(util_dir)

import common_utilities as cu

current_timezone = pendulum.timezone("US/Central")

config_name = "ra_config.yaml"
config = cu.call_config_yaml("ra_config.yaml","hca_ra_default_vars")
timezone = pendulum.timezone("US/Central")


default_args = {
    'owner': 'hca_ra_atos',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2022, 12, 8, tz=timezone),
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
        "param_bqutil_fns_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_bqutil_fns_dataset_name'],
        "param_auth_base_views_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_auth_base_views_dataset_name'],
        "param_parallon_ra_audit_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_parallon_ra_audit_dataset_name'],
        "param_gcs_outbound_bucket" : config['env']['v_gcs_outboundfile_bucket'],
        "param_parallon_pbs_views_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_parallon_pbs_views_dataset_name'],
        "param_fs_base_views_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_fs_base_views_dataset_name'],
        "param_parallon_pbs_base_views_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_parallon_pbs_base_views_dataset_name'],
        "param_parallon_cur_project_id": config['env']['v_curated_project_id']
    }
}

def get_config_file_path(**kwargs):
    config_file_path = kwargs['dag_run'].conf.get('config_file_path')
    return config_file_path

def create_dag(dag_id, schedule, start_date, dependency, source_system, frequency):
    if schedule == "None":
        schedule = None
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        start_date=eval(start_date),
        schedule_interval=schedule,
        catchup=False,
        max_active_runs=1,
        template_searchpath='/home/airflow/gcs/dags/edwra/sql/',
        tags=[f"{source_system}", f"{frequency}"]
    )
    with dag:

        start_job = DummyOperator(task_id='start_job')
        end_dag = DummyOperator(task_id='end_dag')
        table_grp_arr = []
        for ind, item in enumerate(dependency):
            table = item["table"]
            has_sensor = item["has_sensor"]
            has_file_export = item["has_file_export"]
            with TaskGroup(group_id=f'TG-table-{table}') as table_grp:
                # table_task = DummyOperator(task_id=f'T-table-{table}')
                sql_arr = []
                # task for calling audit function

                task_id_core = f'TG-table-{table}.' + f'run_{item["sql"][0]["name"]}'

                for idx, sql in enumerate(item["sql"]):
                    update_bq_table = BigQueryOperator(
                        gcp_conn_id=config['env']['v_curated_conn_id'],
                        sql=f'dml/integration/incremental/{sql["name"]}',
                        use_legacy_sql=False,
                        retries=2,
                        retry_delay=timedelta(minutes=3),
                        task_id=f'run_{sql["name"]}'
                    )

                    update_audit_table = PythonOperator(
                        task_id=f'audit_table_{sql["name"]}',
                        python_callable=cu.executevalidationsqls,
                        provide_context=True,
                        op_kwargs={
                            'bq_table': table,
                            'task_id': task_id_core,
                            'bteq_name': sql["name"],
                            'dag_id': dag_id,
                            'source': source_system,
                            'config': config,
                            'replacements': default_args['params']
                        }
                    )

                    if idx == 0:
                        # table_task >> update_bq_table
                        [update_bq_table]
                    else:
                        sql_arr[-1] >> update_bq_table
                    sql_arr.append(update_bq_table)
                sql_arr[len(sql_arr) - 1] >> update_audit_table 

            if has_sensor == 'Yes':
                    with TaskGroup(group_id=f'TG-sensor-{source_system}') as sensor_grp:
                        for sensor in item["sensor"]:
                            ext_dag_id = sensor["dag_id"]
                            ext_task_id = sensor["task_id"]
                            sen_schedule = sensor["schedule"]
                            check_task_completion = ExternalTaskSensor(
                                task_id=f"Check_status_{ext_dag_id}",
                                external_dag_id=ext_dag_id,
                                external_task_id=ext_task_id,
                                timeout=21600,
                                execution_date_fn=cu.get_execution_date,
                                poke_interval=300,
                                params={"schedule": sen_schedule},
                                # execution_delta=timedelta(minutes=60),
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

                combine_gcs_files = BashOperator(
                    task_id="combine_files",
                    dag = dag,
                    params = {'source_files': source_file_str, 'dest_files': dest_files_str},
                    bash_command="python /home/airflow/gcs/dags/edwra/scripts/combine_files.py --source_files {} --dest_files {} --src_sys_config_file={}".format(source_file_str, dest_files_str, config_name),
                )
                
                file_export_df_job = BashOperator(
                    task_id="file_export_to_server",
                    dag = dag,
                    params = {'source_files': source_file_str, 'dest_files': dest_files_str},
                    # bash_command="python /home/airflow/gcs/dags/edwra/scripts/file_export_df.py --env_config_yaml=edwra/config/ra_config.yaml --dv_config_yaml={{ ti.xcom_pull(task_ids='read_config_file_path') }} --source_files '{{params.source_files}}' --dest_files '{{params.dest_files}}' ",
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

            if ind == 0:
                if has_sensor == 'Yes': 
                    if has_file_export == 'Yes': 
                        start_job >> sensor_grp >> table_grp >> combine_gcs_files >> file_export_df_job >> wait_for_python_job_async_done >> end_dag
                    else: 
                        start_job >> sensor_grp >> table_grp >> end_dag
                else: 
                    if has_file_export == 'Yes': 
                        start_job >> table_grp >> combine_gcs_files >> file_export_df_job >> wait_for_python_job_async_done >> end_dag
                    else: 
                        start_job >> table_grp >> end_dag
            else:
                if has_sensor == 'Yes': 
                    if has_file_export == 'Yes': 
                        table_grp_arr[-1] >> sensor_grp >> table_grp >> combine_gcs_files >> file_export_df_job >> wait_for_python_job_async_done >> end_dag
                    else: 
                        table_grp_arr[-1] >> sensor_grp >> table_grp >> end_dag
                else: 
                    if has_file_export == 'Yes' : 
                        table_grp_arr[-1] >> table_grp >> combine_gcs_files >> file_export_df_job >> wait_for_python_job_async_done >> end_dag
                    else:  
                        table_grp_arr[-1] >> table_grp >> end_dag
            table_grp_arr.append(table_grp)

        #table_grp_arr >> end_dag

    return dag


config_ra_oracle = cu.call_config_yaml("ra_oracle_integrate_dependency.yaml", "ra_oracle_integrate_dependency")
for integrate in config_ra_oracle['integrate']:
    frequency = integrate["frequency"]

    if  integrate["schedule"]  == "0 21 12-31 * 0" : 
        year,month=date.today().year,date.today().month
        sundays=[week[6] for week in calendar.monthcalendar(year,month) if week[6]!=0 and week[6]>12] # Calculates the sundays of the month which falls after 12th of the month
        schedule = "0 21 {} * *".format((','.join(str(a) for a in sundays)))
        time = schedule.split(" ")
        interval_range = time[1].zfill(2) + "." + time[0].zfill(2)
    else: 
        schedule = integrate["schedule"]
 
    start_date = integrate["start_date"]
    source_system = integrate["source_system"]
    type = integrate["type"]
    dependency = integrate["dependency"]

    if  integrate["schedule"] != 'None':
        time = schedule.split(" ")
        interval_range = time[1].zfill(2) + "." + time[0].zfill(2)
        dag_id = f'dag_integrate_' + source_system + '_' + type + '_' + frequency + '_' + interval_range
    else:
        dag_id = f'dag_integrate_' + source_system + '_' + type + '_' + frequency
    globals()[dag_id] = create_dag(dag_id, schedule, start_date, dependency, source_system, frequency)