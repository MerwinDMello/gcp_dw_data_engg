from airflow import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.hooks.dataflow import DataflowJobStatus
from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatusSensor
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
srcsys_config = cu.call_config_yaml(f"{src_system}_polling_ingest_config.yaml", f"{src_system}_polling_ingest")
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

def create_dag(dag_id, schedule, start_date, source_system, db_type, frequency, tags, process_name, polling_type, trigger_dag_ids):
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
            
        #run parallel df jobs
        with TaskGroup(group_id=f'run_dataflow_jobs') as dataflow_group:
            with TaskGroup(group_id=f"TG-df") as tablegroup:
                run_dataflow_job = [BashOperator(
                    task_id=f"run_{source_system}",
                    dag=dag,
                    # to start parallel tasks at random times within 5 minutes, sleep n seconds
                    bash_command=f"sleep $(shuf -i 1-30 -n 1) ; python /home/airflow/gcs/dags/{lob}/scripts/{srcsys_config['v_jdbcbqpytemplate']}"
                    f" --src_sys_config_file={source_system}_polling_ingest_config.yaml"
                    f" --process_name={process_name} --polling_type={polling_type} --beam_runner=DataflowRunner"
                )]

                wait_for_python_job_async_done = DataflowJobStatusSensor(
                    task_id=f"wait_for_python_job_async_done",
                    job_id=f"{{{{ task_instance.xcom_pull(task_ids= 'run_dataflow_jobs.TG-df.run_{source_system}')}}}}",
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
            start_job >> dataflow_group >> trigger_dag_grp >> end_job
        else:
            start_job >> dataflow_group >> end_job

source_system = srcsys_config['v_sourcesysnm']
db_type = srcsys_config['v_databasetype']
for schedule in srcsys_config['schedule']:
    schedule_interval = schedule["schedule_interval"]
    start_date = schedule["start_date"]
    frequency = schedule["frequency"]
    dag_name_suffix = schedule["dag_name_suffix"]
    process_name = schedule["process"]
    polling_type = schedule["polling_type"]
    trigger_dag_ids = schedule["trigger_dag_ids"]

    if schedule_interval  == "None":
        interval_range =  "None"
        schedule = None
    else:
        schedule = schedule_interval
        time = schedule.split(" ")
        interval_range = time[1].zfill(2) + "."  + time[0].zfill(2)

    tags = [source_system, dag_name_suffix]

    if frequency == "None":
        dag_id = f'dag_{polling_type}polling_{source_system}_{db_type}_adhoc_{dag_name_suffix}'
        tags.extend(['adhoc', lob_abbr])
    else:
        dag_id = f'dag_{polling_type}polling_{source_system}_{db_type}_{frequency}_{dag_name_suffix}'
        tags.extend([frequency, lob_abbr])
    
    globals()[dag_id] = create_dag(dag_id, schedule, start_date, source_system, db_type, frequency, tags, process_name, polling_type, trigger_dag_ids)