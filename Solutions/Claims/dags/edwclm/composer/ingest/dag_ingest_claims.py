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
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta
import os
import sys
import time
import pendulum

script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, '../..', 'utils')
sys.path.append(util_dir)
import common_utilities as cu

lob = "edwclm"
lob = lob.lower().strip()
lob_abbr = lob.replace("edw","")
src_system = "claims"
src_system = src_system.lower().strip()
config = cu.call_config_yaml(f"{lob}_config.yaml", f"hca_{lob_abbr}_default_vars")
gcp_conn_id = config['env']['v_curated_conn_id']
bq_project_id = config['env']['v_curated_project_id']
srcsys_config = cu.call_config_yaml(f"{src_system}_ingest_config.yaml", f"{src_system}_ingest")
current_timezone = pendulum.timezone("US/Central")

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

def create_dag(dag_id, schedule, start_date, source_system, db_type, frequency, tags, done_file, tblist, truncatetablelist, has_sensor, sensor_list,trigger_dag_ids):
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
        table_grp_arr = []

        if (done_file != 'NONE'):
            prev = ''
            now= ''
            try:
                if frequency == "daily":
                    now = pendulum.now(current_timezone).strftime("%Y%m%d")
                    prev = (pendulum.now(current_timezone).subtract(days=1)).strftime("%Y%m%d")
                elif frequency == "weekly":
                    now = pendulum.yesterday(current_timezone).strftime("%Y%m%d")
                    prev = (pendulum.yesterday(current_timezone).subtract(days=7)).strftime("%Y%m%d")
            except:
                logging.info(
                    "================={} is not a valid frequency(daily/weekly)=================".format(frequency))
            
            done_file_to_delete =  done_file.replace('YYYYMMDD', prev)     
            done_file= done_file.replace('YYYYMMDD', now)
            file_sensor = GCSObjectsWithPrefixExistenceSensor(
                    bucket=config['env']['v_data_bucket_name'],
                    prefix=config['env']['v_srcfilesdir']  + source_system + "/" + done_file,
                    timeout=600,
                    mode="reschedule",
                    task_id=f"check_{source_system}_done_file_exists"
            )
            
            delete_old_done_files = PythonOperator(
                task_id=f'delete_{source_system}_old_done_files',
                python_callable=cu.removegcsfileifexists,
                op_kwargs={
                    'sourcesysname' : source_system,
                    'folder' : config['env']['v_srcfilesdir'],
                    'filename' : done_file_to_delete
                    }
            )

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

        # these are full table loads, so truncate before load.
        with TaskGroup(group_id='truncate_tables') as truncate_grp:
            for table_name in truncatetablelist:
                truncate_table_sql_statement = f"TRUNCATE TABLE `{bq_project_id}.{'.'.join([config['env'][table_name.split('.')[0]],table_name.split('.')[1]]) if len(table_name.split('.')) > 1 else '.'.join([config['env'][f'v_{lob_abbr}_stage_dataset_name'],table_name.split('.')[0]])}`;"
                truncate_table = BigQueryInsertJobOperator(
                    task_id=f"truncate_table_{table_name.split('.')[1] if len(table_name.split('.')) > 1 else table_name.split('.')[0]}",
                    gcp_conn_id=gcp_conn_id,
                    retries=0,
                    configuration={
                        "query": {
                            "query": truncate_table_sql_statement,
                            "useLegacySql": False,
                        }
                    },
                )

        #run parallel df jobs
        with TaskGroup(group_id=f'run_dataflow_jobs') as dataflow_group:
            for list in tblist:
                with TaskGroup(group_id=f'TG-df_{list}') as tablegroup:

                    run_dataflow_job = [BashOperator(
                        task_id=f"run_{source_system}_{list}",
                        dag=dag,
                        # to start parallel tasks at random times within 5 minutes, sleep n seconds
                        bash_command=f"sleep $(shuf -i 1-30 -n 1) ; python /home/airflow/gcs/dags/{lob}/scripts/{srcsys_config['v_jdbcbqpytemplate']}"
                        f" --src_sys_config_file={source_system}_ingest_config.yaml --src_tbl_list={list} --beam_runner=DataflowRunner"
                    )]

                    wait_for_python_job_async_done = DataflowJobStatusSensor(
                        task_id=f"wait_for_python_job_async_done_{list}",
                        job_id=f"{{{{ task_instance.xcom_pull(task_ids= 'run_dataflow_jobs.TG-df_{list}.run_{source_system}_{list}')}}}}",
                        expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
                        mode="reschedule",
                        poke_interval=300,
                        location=config['env']['v_region'],
                    )

                    run_dataflow_job >> wait_for_python_job_async_done
                [tablegroup]
        
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
            if (done_file != 'NONE'):
                if has_sensor == 'Yes':
                    start_job >> file_sensor >> sensor_grp >> truncate_grp >> dataflow_group >> delete_old_done_files>> trigger_dag_grp >> end_job
                else:
                    start_job >> file_sensor >> truncate_grp >> dataflow_group >> delete_old_done_files>> trigger_dag_grp >> end_job
            else:
                if has_sensor == 'Yes':
                    start_job >> sensor_grp >> truncate_grp >> dataflow_group>> trigger_dag_grp >> end_job
                else:
                    start_job >> truncate_grp >> dataflow_group>> trigger_dag_grp >> end_job
        else:
            if (done_file != 'NONE'):
                if has_sensor == 'Yes':
                    start_job >> file_sensor >> sensor_grp >> truncate_grp >> dataflow_group >> delete_old_done_files >> end_job
                else:
                    start_job >> file_sensor >> truncate_grp >> dataflow_group >> delete_old_done_files >> end_job
            else:
                if has_sensor == 'Yes':
                    start_job >> sensor_grp >> truncate_grp >> dataflow_group >> end_job
                else:
                    start_job >> truncate_grp >> dataflow_group >> end_job

source_system = srcsys_config['v_sourcesysnm']
db_type = srcsys_config['v_databasetype']
for schedule in srcsys_config['schedule']:
    frequency = schedule["frequency"]
    trigger_dag_ids = schedule["trigger_dag_ids"]
    schedule_interval = schedule["schedule_interval"]
    start_date = schedule["start_date"]
    dag_name_suffix = schedule["dag_name_suffix"]
    done_file = schedule["done_file"]
    tblist = schedule["tblist"]
    truncatetablelist = schedule["truncatetablelist"]
    has_sensor = schedule["has_sensor"]
    if "sensor" in schedule:
        sensor_list = schedule["sensor"]
    else:
        sensor_list = None

    if schedule_interval  == "None":
        interval_range =  "None"
        schedule = None
    else:
        schedule = schedule_interval
        time = schedule.split(" ")
        interval_range = time[1].zfill(2) + "."  + time[0].zfill(2)

    tags = [source_system, dag_name_suffix]

    if frequency == "None":
        dag_id = f'dag_ingest_{source_system}_{db_type}_adhoc_{dag_name_suffix}'
        tags.extend(['adhoc', lob_abbr])
    else:
        dag_id = f'dag_ingest_{source_system}_{db_type}_{frequency}_{dag_name_suffix}'
        tags.extend([frequency, lob_abbr])
    
    globals()[dag_id] = create_dag(dag_id, schedule, start_date, source_system, db_type, frequency, tags, done_file, tblist, truncatetablelist, has_sensor, sensor_list, trigger_dag_ids)