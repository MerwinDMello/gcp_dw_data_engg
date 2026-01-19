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

script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, '..', 'utils')
sys.path.append(util_dir)
import common_utilities as cu

lob = "edwra"
lob = lob.lower().strip()
lob_abbr = lob.replace("edw","")
source_system = "govreports"
source_system = source_system.lower().strip()
config = cu.call_config_yaml(f"{lob_abbr}_config.yaml", f"hca_{lob_abbr}_default_vars")
config_type = "outbound"
gcp_conn_id = config['env']['v_curated_conn_id']
bq_project_id = config['env']['v_curated_project_id']
df_template = config['env']['v_file_df_template']
outbound_config = cu.call_config_yaml(f"ra_{source_system}_{config_type}_config.yaml", f"{lob_abbr}_{source_system}_{config_type}")
timezone = pendulum.timezone("US/Central")

default_args = {
    'owner': f'hca_{lob_abbr}_atos',
    'depends_on_past': False,
    'email': config['env']['v_failure_email_list'],
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=480)
}

def create_dag(dag_id, schedule, start_date, dag_name_suffix, source_system, frequency, tags, done_file, has_sensor, sensor_list, trigger_dag_ids, process_groups, outbound_process):
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
                    timeout=18000,
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

        if "outbound_files" in outbound_process:
            outbound_files = outbound_process["outbound_files"]
        else:
            outbound_files = None

        #run parallel df jobs for loading files
        if outbound_files:
            with TaskGroup(group_id=f'run_outbound_files_dataflow_jobs') as outbound_files_dataflow_group:
                for process_group in process_groups:
                    with TaskGroup(group_id=f'TG-df_{source_system}_{process_group.lower()}') as tablegroup:
                        for outbound_file in outbound_files:
                            file_name_mnemonic = outbound_file["file_name_mnemonic"]

                            run_outbound_files_dataflow_job = [BashOperator(
                                task_id=f"run_outbound_files_{file_name_mnemonic.lower()}",
                                dag=dag,
                                # to start parallel tasks at random times within 5 minutes, sleep n seconds
                                bash_command=f"sleep $(shuf -i 1-30 -n 1) ; python /home/airflow/gcs/dags/{lob}/scripts/{df_template}"
                                f" --src_system={source_system} --dag_name_suffix={dag_name_suffix} --beam_runner=DataflowRunner"
                                f" --file_name_mnemonic={file_name_mnemonic} --process_type=Outbound_Files --config_type={config_type}"
                                f" --process_group={process_group}"
                            )]

                            wait_for_python_outbound_files_job_async_done = DataflowJobStatusSensor(
                                task_id=f"wait_for_python_outbound_job_async_done_{file_name_mnemonic.lower()}",
                                job_id=f"{{{{ task_instance.xcom_pull(task_ids= 'run_outbound_files_dataflow_jobs.TG-df_{source_system}_{process_group.lower()}.run_outbound_files_{file_name_mnemonic.lower()}')}}}}",
                                expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
                                mode="reschedule",
                                poke_interval=120,
                                location=config['env']['v_region'],
                            )
                        
                            run_outbound_files_dataflow_job >> wait_for_python_outbound_files_job_async_done
                        [tablegroup]

        if "convert_files" in outbound_process:
            convert_files = outbound_process["convert_files"]
        else:
            convert_files = None

        #run parallel df jobs for converting files
        if convert_files:
            with TaskGroup(group_id=f'run_convert_files_dataflow_jobs') as convert_files_dataflow_group:
                for process_group in process_groups:
                    with TaskGroup(group_id=f'TG-df_{source_system}_{process_group.lower()}') as tablegroup:
                        for convert_file in convert_files:
                            file_name_mnemonic = convert_file["file_name_mnemonic"]

                            run_convert_files_dataflow_job = [BashOperator(
                                task_id=f"run_convert_files_{file_name_mnemonic.lower()}",
                                dag=dag,
                                # to start parallel tasks at random times within 5 minutes, sleep n seconds
                                bash_command=f"sleep $(shuf -i 1-30 -n 1) ; python /home/airflow/gcs/dags/{lob}/scripts/{df_template}"
                                f" --src_system={source_system} --dag_name_suffix={dag_name_suffix} --beam_runner=DataflowRunner"
                                f" --file_name_mnemonic={file_name_mnemonic} --process_type=Convert_Files --config_type={config_type}"
                                f" --process_group={process_group}"
                            )]

                            wait_for_python_convert_files_job_async_done = DataflowJobStatusSensor(
                                task_id=f"wait_for_python_convert_job_async_done_{file_name_mnemonic.lower()}",
                                job_id=f"{{{{ task_instance.xcom_pull(task_ids= 'run_convert_files_dataflow_jobs.TG-df_{source_system}_{process_group.lower()}.run_convert_files_{file_name_mnemonic.lower()}')}}}}",
                                expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
                                mode="reschedule",
                                poke_interval=120,
                                location=config['env']['v_region'],
                            )
                        
                            run_convert_files_dataflow_job >> wait_for_python_convert_files_job_async_done
                        [tablegroup]

        
        if trigger_dag_ids:
            with TaskGroup(group_id=f'TG-trigger_dag') as trigger_dag_grp:
                for trigger_dag_id in trigger_dag_ids:
                        trigger_dag = TriggerDagRunOperator(
                        task_id=f"trigger_dag_{trigger_dag_id}",
                        trigger_dag_id=trigger_dag_id,
                )
            [trigger_dag_grp]

        last_task = start_job

        if (done_file != 'NONE'):
            last_task.set_downstream(file_sensor)
            last_task = file_sensor

        if has_sensor == 'Yes':
            last_task.set_downstream(sensor_grp)
            last_task = sensor_grp

        if outbound_files:
            last_task.set_downstream(outbound_files_dataflow_group)
            last_task = outbound_files_dataflow_group

        if convert_files:
            last_task.set_downstream(convert_files_dataflow_group)
            last_task = convert_files_dataflow_group

        if (done_file != 'NONE'):
            last_task.set_downstream(delete_old_done_files)
            last_task = delete_old_done_files

        if trigger_dag_ids:
            last_task.set_downstream(trigger_dag_grp)
            last_task = trigger_dag_grp

        last_task.set_downstream(end_job)

for outbound_process in outbound_config[source_system]:
    start_date = outbound_process["start_date"]
    schedule_interval = outbound_process["schedule"]
    frequency = outbound_process["frequency"]
    db_type = outbound_process['datasourcetype']
    dag_name_suffix = outbound_process["dag_name_suffix"]
    process_groups = outbound_process["process_groups"]
    trigger_dag_ids = outbound_process["trigger_dag_ids"]
    done_file = outbound_process["done_file"]
    has_sensor = outbound_process["has_sensor"]
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
        dag_id = f'dag_{config_type}_{source_system}_{db_type}_adhoc_{dag_name_suffix}'
        tags.extend(['adhoc', lob_abbr])
    else:
        dag_id = f'dag_{config_type}_{source_system}_{db_type}_{frequency}_{dag_name_suffix}'
        tags.extend([frequency, lob_abbr])
    
    globals()[dag_id] = create_dag(dag_id, schedule, start_date, dag_name_suffix, source_system, frequency, tags, done_file, has_sensor, sensor_list, trigger_dag_ids, process_groups, outbound_process)