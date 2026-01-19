from airflow import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.dataflow import DataflowJobStatus
from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatusSensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta
import os
import sys
import time
import pendulum
import logging

script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, '..','utils')
sys.path.append(util_dir)
import common_utilities as cu

lob = "edwra"
lob = lob.lower().strip()
lob_abbr = lob.replace("edw","")
source_system = "ra"
source_system = source_system.lower().strip()    
config = cu.call_config_yaml(f"{lob_abbr}_config.yaml", f"hca_{lob_abbr}_default_vars")
gcp_conn_id = config['env']['v_curated_conn_id']
bq_project_id = config['env']['v_curated_project_id']
outbound_config = cu.call_config_yaml(f"{source_system}_external_data_reporting_config.yaml", f"{source_system}_reporting_outbound")
timezone = pendulum.timezone("US/Central")
ra_config_file_name = "ra_config.yaml"


default_args = {
    'owner': f'hca_{lob_abbr}_atos',
    'depends_on_past': False,
    'email': False,
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=480)
}

def create_dag(dag_id, schedule, start_date, dag_id_suffix, source_system, frequency, tags, has_sensor, sensor_list, trigger_dag_ids, tgttablename):
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        start_date=eval(start_date),
        schedule_interval=schedule,
        is_paused_upon_creation=True,
        catchup=False,
        max_active_runs=1,
        max_active_tasks=18,
        concurrency=18,
        tags=tags
    )



    with dag:

        start_job = DummyOperator(task_id='start_dag')
        end_job = DummyOperator(task_id='end_dag')

        if has_sensor == 'Yes':
            with TaskGroup(group_id=f'TG-sensor-{source_system}') as sensor_grp:
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

        #run df jobs
        df_template = outbound_config['v_jdbcbqpytemplate']

        with TaskGroup(group_id=f'run_dataflow_jobs') as dataflow_group:
            #tablegroup=None
            for script, target_table in zip(sql_label, tgttablename):
                #sanitized_target_table = sanitize_table_name(target_table)
                schema = outbound_config[script].split("~")[2]
                logging.info("Sql query: {} target_table: {}" .format(script,target_table))
                with TaskGroup(group_id=f'TG-df_{target_table}_{schema}_writeback') as tablegroup:                
                    run_dataflow_job = [BashOperator(
                        task_id=f"run_{source_system}_{target_table}_{schema}_writeback",
                        dag=dag,
                    # to start parallel tasks at random times within 5 minutes, sleep n seconds
                         bash_command=f"sleep $(shuf -i 1-30 -n 1) ; python /home/airflow/gcs/dags/{lob}/scripts/{df_template}"
                            f" --src_sys_config_file=ra_external_data_reporting_config.yaml --src_system={source_system}"
                            f" --tgttablename={target_table} --src_db_sql={script}"
                            f" --beam_runner=DataflowRunner --schema={schema}"
                )]

                    wait_for_python_job_async_done = DataflowJobStatusSensor(
                        task_id=f"wait_for_python_job_async_done_{target_table}_{schema}_writeback",
                        job_id=f"{{{{ task_instance.xcom_pull(task_ids= 'run_dataflow_jobs.TG-df_{target_table}_{schema}_writeback.run_{source_system}_{target_table}_{schema}_writeback')}}}}",
                        expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
                        mode="reschedule",
                        poke_interval=300,
                        location=config['env']['v_region'],
                )

                run_dataflow_job >> wait_for_python_job_async_done
            [tablegroup]
        
        if has_file_extract is True:
            # Trigger done file export to server
            file_export_df_job = BashOperator(
                task_id="file_export_to_server",
                dag = dag,
                bash_command="python /home/airflow/gcs/dags/edwra/scripts/file_export_df.py --source_files {} --dest_files {} --src_sys_config_file={}".format(source_file_name, destination_file_name, ra_config_file_name),
            )

            # Sensor that checks Dataflow job status every 1 minute
            # There are options to continuously check for status but this way clears up slots
            wait_for_done_file_job_async_done = DataflowJobStatusSensor(
                task_id="wait_for_done_file_job_async_done",
                job_id=f"{{{{ task_instance.xcom_pull(task_ids= 'file_export_to_server')}}}}",
                expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
                mode="reschedule",
                poke_interval= 30,
                location='us-east4',
            )
        
        if trigger_dag_ids:
            with TaskGroup(group_id=f'TG-trigger_dag') as trigger_dag_grp:
                for trigger_dag_id in trigger_dag_ids:
                        trigger_dag = TriggerDagRunOperator(
                        task_id=f"trigger_dag_{trigger_dag_id}",
                        trigger_dag_id=trigger_dag_id,
                )
            [trigger_dag_grp]

        # run dag
        if trigger_dag_ids:
            if has_sensor == 'Yes':
                start_job >> sensor_grp >> dataflow_group >> file_export_df_job >> wait_for_done_file_job_async_done >> trigger_dag_grp >> end_job
            else:
                start_job >> dataflow_group >> file_export_df_job >> wait_for_done_file_job_async_done >> trigger_dag_grp >> end_job
        else:
            if has_sensor == 'Yes':
                start_job >> sensor_grp >> dataflow_group >> file_export_df_job >> wait_for_done_file_job_async_done >> end_job
            else:
                start_job >> dataflow_group >> file_export_df_job >> wait_for_done_file_job_async_done >> end_job


for outbound_process in outbound_config[source_system]:
    start_date = outbound_process["start_date"]
    frequency = outbound_process["frequency"]
    trigger_dag_ids = None
    schedule_interval = outbound_process["schedule_interval"]
    dag_id_suffix = str(outbound_process["dag_id_suffix"]).zfill(2)
    done_file = outbound_process["done_file"]
    has_sensor = outbound_process["has_sensor"]
    sql_label = outbound_process["sql"]
    tgttablename = outbound_process['tgttablename']
    has_file_extract = outbound_process["has_file_extract"]
    if outbound_process["has_file_extract"] is True:
        source_file_name = outbound_process["source_file_name"]
        destination_file_name = outbound_process["destination_file_name"]
    else:
        source_file_name = None
        destination_file_name = None

    if "sensor" in outbound_process:
        sensor_list = outbound_process["sensor"]
    else:
        sensor_list = None

    if schedule_interval  == "None":
        interval_range =  "None"
        schedule = None
    else:
        schedule = schedule_interval
        time = schedule.split(" ")
        interval_range = time[1].zfill(2) + "."  + time[0].zfill(2)

    tags = [source_system]

    if frequency == "None":
        dag_id = f'dag_outbound_{source_system}_oracle_adhoc_{dag_id_suffix}'
        tags.extend(['adhoc', lob_abbr])
    else:
        dag_id = f'dag_outbound_{source_system}_oracle_{frequency}_{dag_id_suffix}'
        tags.extend([frequency, lob_abbr])
    
    globals()[dag_id] = create_dag(dag_id, schedule, start_date, dag_id_suffix, source_system, frequency, tags, has_sensor, sensor_list, trigger_dag_ids, tgttablename)