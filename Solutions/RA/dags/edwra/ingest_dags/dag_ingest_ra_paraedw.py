from airflow import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.hooks.dataflow import DataflowJobStatus
from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatusSensor
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from datetime import datetime, timedelta
import os
import sys
import time
import pendulum

script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, '..', 'utils')
sys.path.append(util_dir)
import common_utilities as cu

current_timezone = pendulum.timezone("US/Central")

lob = "edwra"
lob = lob.lower().strip()
lob_abbr = lob.replace("edw","")
src_system = "paraedw"
src_system = src_system.lower().strip()
config = cu.call_config_yaml(f"{lob_abbr}_config.yaml", f"hca_{lob_abbr}_default_vars")
gcp_conn_id = config['env']['v_curated_conn_id']
bq_project_id = config['env']['v_curated_project_id']
srcsys_config = cu.call_config_yaml(f"ra_{src_system}_ingest_config.yaml", f"ra_{src_system}_ingest")
server_db_config = cu.call_config_yaml(f"ra_{src_system}_server_schema.yaml", f"ra_{src_system}_server_schema")
timezone = pendulum.timezone("US/Central")

def calculate_v_from(**kwargs):
    execution_date = kwargs['execution_date']
    if execution_date.weekday() == 6:  # Sunday is 6
        v_from = '1900-01-01'
        print(f"Execution date: {execution_date}, v_from: {v_from}")
        return v_from
    else:
        v_from = (execution_date - timedelta(days=5)).strftime("%Y-%m-%d")  # Subtract 5 days and format the date
        print(f"Execution date: {execution_date}, v_from: {v_from}")
        return v_from

default_args = {
    'owner': 'hca_ra_atos',
    'depends_on_past': False,
    'email': config['env']['v_failure_email_list'],
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=480)
}

def create_dag(dag_id, schedule, start_date, source_system, db_type, frequency, tags, done_files, tblist, truncatetablelist):
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        start_date=eval(start_date),
        schedule_interval=schedule,
        catchup=False,
        max_active_runs=1,
        max_active_tasks=10,
        concurrency=10,
        tags=tags
    )

    with dag:

        start_job = DummyOperator(task_id='start_dag')
        end_job = DummyOperator(task_id='end_dag')
        table_grp_arr = []

        # Calculate v_from_date
        v_from_date_task = PythonOperator(
            task_id='calculate_v_from',
            python_callable=calculate_v_from,
            provide_context=True
        )

        if (done_files != 'NONE'):
            now = pendulum.now(current_timezone).strftime("%Y%m%d")
            prev = (pendulum.now(current_timezone).subtract(days=1)).strftime("%Y%m%d")
            with TaskGroup(group_id=f'TG-ra-paraedw-done-files-check') as done_file_grp:
                for ind, item in enumerate(done_files):
                    done_file_name = item
                    done_file_to_delete =  done_file_name.replace('YYYYMMDD', prev)
                    done_file_to_check= done_file_name.replace('YYYYMMDD', now)
                    file_sensor = GCSObjectsWithPrefixExistenceSensor(
                            bucket=config['env']['v_data_bucket_name'],
                            prefix=config['env']['v_srcfilesdir']  + "oracle" + "/" + done_file_to_check,
                            timeout=18000,
                            mode="reschedule",
                            task_id=f"check_done_file_{done_file_name}_exists"
                    )
                    delete_old_done_files = PythonOperator(
                        task_id=f'delete_old_done_file_{done_file_name}',
                        python_callable=cu.removegcsfileifexists,
                        op_kwargs={
                            'sourcesysname' : "oracle",
                            'folder' : config['env']['v_srcfilesdir'],
                            'filename' : done_file_to_delete
                            }
                    )
                    [file_sensor >> delete_old_done_files]

        # these are full table loads, so truncate before load.
        with TaskGroup(group_id='truncate_tables') as truncate_grp:            
            truncate_table_list = [f"TRUNCATE TABLE `{bq_project_id}.{config['env'][f'v_parallon_{lob_abbr}_stage_dataset_name']}.{table_name}`;" for table_name in truncatetablelist]
            truncate_table = BigQueryExecuteQueryOperator(
                    task_id=f"truncate_tables",
                    dag=dag,
                    sql=truncate_table_list,
                    use_legacy_sql=False,
                    gcp_conn_id=gcp_conn_id
            )

        #run parallel df jobs
        for ind,srvr in enumerate(server_db_config[f'{source_system}']):
            server_name = srvr['server']
            server_name_formatted = server_name.replace(".","_").lower()
            db_list = srvr['schema']
            with TaskGroup(group_id=f'run_dataflow_jobs_{server_name_formatted}') as dataflow_group:
                for list in tblist:
                    with TaskGroup(group_id=f'TG-td_df_{list}') as tablegroup:

                        run_dataflow_job = [BashOperator(
                            task_id=f"run_{source_system}_{list}",
                            dag=dag,
                            # to start parallel tasks at random times within 5 minutes, sleep n seconds
                            bash_command=f"sleep $(shuf -i 1-300 -n 1) ; python /home/airflow/gcs/dags/edwra/scripts/{srcsys_config['v_jdbcbqpytemplate']}"
                            f" --src_sys_config_file=ra_{source_system}_ingest_config.yaml --src_tbl_list={list} --src_server_name={server_name}"
                            f" --src_from_date={v_from_date_task.output} --src_system={source_system} --src_db_list=\'{db_list}\' --src_db_type={db_type}"
                            " --beam_runner=DataflowRunner"
                        )]

                        wait_for_python_job_async_done = DataflowJobStatusSensor(
                            task_id=f"wait_for_python_job_async_done_{list}",
                            job_id=f"{{{{ task_instance.xcom_pull(task_ids= 'run_dataflow_jobs_{server_name_formatted}.TG-td_df_{list}.run_{source_system}_{list}')}}}}",
                            expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
                            mode="reschedule",
                            poke_interval=300,
                            location=config['env']['v_region'],
                        )

                        run_dataflow_job >> wait_for_python_job_async_done
                    [tablegroup]

        # run dag
        if (done_files != 'NONE'):
            start_job >> done_file_grp  >> v_from_date_task >> truncate_grp >> dataflow_group >> end_job
        else:
            start_job >> v_from_date_task >> truncate_grp >> dataflow_group >> end_job

source_system = srcsys_config['v_sourcesysnm']
db_type = srcsys_config['v_databasetype']

for schedule in srcsys_config['schedule']:
    frequency = schedule["frequency"]
    schedule_interval = schedule["schedule_interval"]
    start_date = schedule["start_date"]
    done_files = schedule["done_file"]
    tblist = schedule["tblist"]
    truncatetablelist=schedule["truncatetablelist"]

    if schedule_interval  == "None":
        interval_range =  "None"
        schedule = None
    else:
        schedule = schedule_interval
        time = schedule.split(" ")
        interval_range = time[1].zfill(2) + "."  + time[0].zfill(2)

    tags = [source_system]

    if frequency == "None":
        dag_id = f'dag_ingest_ra_{source_system}_{db_type}_adhoc'
        tags.extend(['adhoc','ra'])
    else:
        dag_id = f'dag_ingest_ra_{source_system}_{db_type}_{frequency}_{interval_range}'
        tags.extend([frequency,'ra'])
    
    globals()[dag_id] = create_dag(dag_id, schedule, start_date, source_system, db_type, frequency, tags, done_files, tblist, truncatetablelist)