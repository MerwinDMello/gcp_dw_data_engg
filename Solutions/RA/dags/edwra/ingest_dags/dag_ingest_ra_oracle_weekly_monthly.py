from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta
from datetime import date
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatusSensor
from airflow.providers.google.cloud.hooks.dataflow import DataflowJobStatus
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
import os
import sys
import pendulum
from airflow.utils.task_group import TaskGroup
import logging
import calendar
from airflow.sensors.external_task import ExternalTaskSensor


script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, '..', 'utils')
sys.path.append(util_dir)
import common_utilities as cu

current_timezone = pendulum.timezone("US/Central")

config = cu.call_config_yaml("ra_config.yaml","hca_ra_default_vars")
ingest_config_name = 'ra_oracle_ingest_dependency_weekly_monthly.yaml'
config_ra_oracle = cu.call_config_yaml(f"{ingest_config_name}","ra_oracle_ingest_dependency_weekly_monthly")
timezone = pendulum.timezone("US/Central")

default_args = {
    'owner': 'hca_ra_atos',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2022, 12, 20, tz=timezone),
    'email': config['env']['v_failure_email_list'],
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=240),
    'params' : {
        # "param_clinical_ci_stage_dataset_name": config['env']['v_curated_project_id'] +
        "param_parallon_ra_stage_dataset_name": config['env']['v_curated_project_id'] +
                                                        '.' + config['env']['v_parallon_ra_stage_dataset_name']
    }
}


def create_dag(dag_id, schedule, start_date, source_system, source_db, frequency, done_files, has_sensor, sensor_list, tblist, src_schema_id, trigger_dag_ids):
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        start_date=eval(start_date),
        schedule_interval=schedule,
        catchup=False,
        max_active_runs=1,
        template_searchpath='/home/airflow/gcs/dags/edwra/sql/',
        tags=["ra", f"{source_system}", f"{frequency}"]
    )
    with dag:
        start_job = DummyOperator(task_id='start_dag')
        end_job = DummyOperator(task_id='end_dag')
        tab_grp_arr = []

        now = pendulum.now(timezone).strftime("%Y%m%d")

        if done_files != "None":
            now = pendulum.now(current_timezone).strftime("%Y%m%d")
            prev = (pendulum.now(current_timezone).subtract(days=1)).strftime("%Y%m%d")
            with TaskGroup(group_id=f'TG-{source_db}-done-files-check') as done_file_grp:
                for ind, item in enumerate(done_files):
                    # done_file_name = item.replace('$SCHEMA', schema)
                    done_file_name = item
                    done_file_to_delete =  done_file_name.replace('YYYYMMDD', prev)
                    done_file_to_check= done_file_name.replace('YYYYMMDD', now)
                    file_sensor = GCSObjectsWithPrefixExistenceSensor(
                            bucket=config['env']['v_data_bucket_name'],
                            prefix=config['env']['v_srcfilesdir']  + source_db + "/" + done_file_to_check,
                            timeout=18000,
                            mode="reschedule",
                            task_id=f"check_done_file_{done_file_name}_exists"
                    )
                    delete_old_done_files = PythonOperator(
                        task_id=f'delete_old_done_file_{done_file_name}',
                        python_callable=cu.removegcsfileifexists,
                        op_kwargs={
                            'sourcesysname' : source_db,
                            'folder' : config['env']['v_srcfilesdir'],
                            'filename' : done_file_to_delete
                            }
                    )
                    [file_sensor >> delete_old_done_files]
        
        if has_sensor:
            with TaskGroup(group_id=f'TG-sensor-{source_db}') as sensor_grp:
                for sensor in sensor_list:
                    ext_dag_id = sensor["dag_id"]
                    ext_task_id = sensor["task_id"]
                    schedule = sensor["schedule"]
                    if "cycle_age" in sensor:
                        cycle_age = sensor["cycle_age"]
                    else:
                        cycle_age = "current"
                    check_task_completion= ExternalTaskSensor(
                        task_id=f"Check_status_{ext_dag_id}",
                        external_dag_id=ext_dag_id,
                        external_task_id=ext_task_id,
                        timeout=10800,
                        execution_date_fn=cu.get_execution_date,
                        params={"schedule":schedule,"frequency":frequency,"cycle_age":cycle_age},
                        allowed_states=["success"],
                        failed_states=["failed", "skipped"],
                        mode="reschedule"
                    )
                    [check_task_completion]

        # Trigger the dependent DAGs passed from the ingest configuration file
        if trigger_dag_ids:
            with TaskGroup(group_id=f'TG-trigger_dag') as trigger_dag_grp:
                for trigger_dag_id in trigger_dag_ids:
                        trigger_dag = TriggerDagRunOperator(
                        task_id=f"trigger_dag_{trigger_dag_id}",
                        trigger_dag_id=trigger_dag_id,
                )
            [trigger_dag_grp]

        with TaskGroup(group_id='run_dataflow_jobs') as tg2:
            for list in tblist:
                with TaskGroup(group_id=f'TG-oracle_df_{list}') as tablegroup:
                    run_dataflow_job = [BashOperator(
                        task_id=f"run_plus_oracle_{list}",
                        dag=dag,
                        bash_command='sleep $(shuf -i 1-30 -n 1) ; python /home/airflow/gcs/dags/edwra/scripts/{} --src_sys_config_file={} --src_sys_airflow_varname=ra_oracle_ingest_dependency_weekly_monthly --src_tbl_list={} --src_schema_id={}'.format(
                            config_ra_oracle['v_jdbcbqpytemplate'], ingest_config_name, list, src_schema_id)
                    )]

                    wait_for_python_job_async_done = DataflowJobStatusSensor(
                        task_id="wait_for_python_job_async_done",
                        job_id=f"{{{{ task_instance.xcom_pull(task_ids= 'run_dataflow_jobs.TG-oracle_df_{list}.run_plus_oracle_{list}')}}}}",
                        expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
                        mode="reschedule",
                        poke_interval=300,
                        location='us-east4',
                    )
                run_dataflow_job >> wait_for_python_job_async_done
            [tablegroup]

        if trigger_dag_ids:
            if done_files != 'None':
                if has_sensor:
                    start_job >> done_file_grp >> sensor_grp >> tg2 >> trigger_dag_grp >> end_job
                else:
                    start_job >> done_file_grp >> tg2 >> trigger_dag_grp >> end_job
            else:
                if has_sensor:
                    start_job >> sensor_grp >> tg2 >> trigger_dag_grp >> end_job
                else:
                    start_job >> tg2 >> trigger_dag_grp >> end_job                
        else:
            if done_files != 'None':
                if has_sensor:
                    start_job >> done_file_grp >> sensor_grp >> tg2 >> end_job
                else:
                    start_job >> done_file_grp >> tg2 >> end_job
            else:
                if has_sensor:
                    start_job >> sensor_grp >> tg2 >> end_job
                else:
                    start_job >> tg2 >> end_job
        return dag

source_system = config_ra_oracle['v_sourcesysnm']
source_db = config_ra_oracle['v_databasetype']
for schedule in config_ra_oracle['schedule']:
    frequency = schedule["frequency"]
    schedule_interval = schedule["v_schedule_interval"]
    start_date = schedule["start_date"]
    src_instance = schedule["src_instance"]
    tblist = schedule["tblist"]
    type = config_ra_oracle['v_databasetype']
    done_files = schedule["done_files"]
    has_sensor = True if schedule["has_sensor"] == "Yes" else False
    sensor_list = schedule["sensor"] if has_sensor else None
    trigger_dag_ids = schedule["trigger_dag_ids"] if "trigger_dag_ids" in schedule else None    

    if schedule_interval == "None":

        interval_range = "None"
        schedule = None

    elif schedule_interval == "0 21 12-31 * 0": 
        year,month=date.today().year,date.today().month
        sundays=[week[6] for week in calendar.monthcalendar(year,month) if week[6]!=0 and week[6]>12] # Calculates the sundays of the month which falls on or after 12th of the month
        schedule = "0 21 {} * *".format((','.join(str(a) for a in sundays))) 
        time = schedule.split(" ")
        interval_range = time[1].zfill(2) + "." + time[0].zfill(2) 

    else:
        schedule = schedule_interval
        time = schedule.split(" ")
        interval_range = time[1].zfill(2) + "." + time[0].zfill(2)

    for src_instance in src_instance:
        if src_instance == 'p1':
            src_schema_id = 1
        elif src_instance == 'p2':
            src_schema_id = 3

        if interval_range == "None":
            dag_id = f'dag_ingest_' + source_system + '_' + src_instance + '_' + type + '_' + frequency
        else:
            dag_id = f'dag_ingest_' + source_system + '_' + src_instance + '_' + type + '_' + frequency + '_' + interval_range
        globals()[dag_id] = create_dag(dag_id, schedule, start_date, source_system, source_db, frequency, done_files, has_sensor, sensor_list, tblist, src_schema_id, trigger_dag_ids)