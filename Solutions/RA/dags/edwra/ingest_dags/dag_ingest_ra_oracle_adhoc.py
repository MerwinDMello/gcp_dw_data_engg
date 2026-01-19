from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
# from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatusSensor
from airflow.providers.google.cloud.hooks.dataflow import DataflowJobStatus
# from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
import os
import sys
import pendulum
from airflow.utils.task_group import TaskGroup
import logging

script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, '..', 'utils')
sys.path.append(util_dir)
import common_utilities as cu

current_timezone = pendulum.timezone("US/Central")

# Get generic config file for ra (same across all sources)
ra_config_file_name = "ra_config.yaml"
config = cu.call_config_yaml(ra_config_file_name,"hca_ra_default_vars")
ingest_config_name = 'ra_oracle_ingest_dependency_adhoc.yaml'
# Get source-specific config file
config_ra_oracle = cu.call_config_yaml(f"{ingest_config_name}","ra_oracle_ingest_dependency_adhoc")
timezone = pendulum.timezone("US/Central")

adhoc_pull_table_var = "ra_adhoc_pull_tables"

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
    'execution_timeout': timedelta(minutes=240)
}


def create_dag(dag_id, schedule, start_date, source_system, source_db, frequency, done_files, has_sensor, sensor_list, tblists, has_adhoc_pull, has_file_extract, source_file_name, destination_file_name, schema,schedule_interval):
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        start_date=eval(start_date),
        schedule_interval=schedule,
        catchup=False,
        is_paused_upon_creation=True, #TODO: Remove
        max_active_runs=1,
        template_searchpath='/home/airflow/gcs/dags/edwra/sql/',
        tags=[f"{source_system}", f"{frequency}", "ra"]
    )
    with dag:
        # Set schema_id variable to pass in to Dataflow job
        if schema == 'p1':
            src_schema_id = 1
        elif schema == 'p2':
            src_schema_id = 3
        # define tasks
        start_job = DummyOperator(task_id='start_dag')
        end_job = DummyOperator(task_id='end_dag')

        now = pendulum.now(timezone).strftime("%Y%m%d")
        # If there are external dependencies, it will check the GCS bucket for the presence of these done files
        if done_files != "None":
            now = pendulum.now(current_timezone).strftime("%Y%m%d")
            prev = (pendulum.now(current_timezone).subtract(days=1)).strftime("%Y%m%d")
            with TaskGroup(group_id=f'TG-{source_db}-done-files-check') as done_file_grp:
                # There may be multiple external dependencies, so a separate task will be created for each one
                for ind, item in enumerate(done_files):
                    done_file_name = item.replace('$SCHEMA', schema)
                    done_file_to_delete =  done_file_name.replace('YYYYMMDD', prev)
                    done_file_to_check= done_file_name.replace('YYYYMMDD', now)
                    # Checks for done file in GCS bucket; done file must be from the same day
                    file_sensor = GCSObjectsWithPrefixExistenceSensor(
                            bucket=config['env']['v_data_bucket_name'],
                            prefix=config['env']['v_srcfilesdir']  + source_db + "/" + done_file_to_check,
                            timeout=18000,
                            mode="reschedule",
                            task_id=f"check_done_file_{done_file_name}_exists"
                    )
                    # To declutter the GCS bucket, old done files are deleted
                    delete_old_done_files = PythonOperator(
                        task_id=f'delete_old_done_file_{done_file_name}',
                        python_callable=cu.removegcsfileifexists,
                        op_kwargs={
                            'sourcesysname' : source_db,
                            'folder' : config['env']['v_srcfilesdir'],
                            'filename' : done_file_to_delete
                            }
                    )
                    #Run the sensors parallely
                    [file_sensor >> delete_old_done_files]       
        
        # Update Airflow variable with adhoc tables list
        if has_adhoc_pull is True:
            update_adhoc_pull_var = PythonOperator(
                task_id='update_adhoc_pull_tables',
                python_callable=cu.update_adhoc_airflow_var,
                op_kwargs = {"airflow_var_name": adhoc_pull_table_var}
            )

        potential_adhoc_pull_tables:list = Variable.get(adhoc_pull_table_var, default_var = [], deserialize_json=True)
        if potential_adhoc_pull_tables == []:
            cu.update_adhoc_airflow_var(airflow_var_name=adhoc_pull_table_var)
        potential_adhoc_pull_tables = [x for x in potential_adhoc_pull_tables if x['schema_id'] == src_schema_id]
        # Tables need to be sorted to keep same order when tasks are dynamically created
        potential_adhoc_pull_tables.sort(key=lambda x: x['table_id'])
          
        if has_adhoc_pull is True:
            with TaskGroup(group_id='run_dataflow_adhoc_pull_jobs') as tg_adhoc_pull:
                for table_info in potential_adhoc_pull_tables:
                    table_id = table_info['table_id']
                    is_adhoc_pull = table_info['is_adhoc_pull']
                    with TaskGroup(group_id=f'TG-oracle_df_{table_id}_adhoc_pull') as tablegroup_adhoc:
                        
                        if is_adhoc_pull is True:
                            run_dataflow_job_adhoc_pull = [BashOperator(
                                task_id=f"run_plus_oracle_{table_id}",
                                dag=dag,
                                bash_command='sleep $(shuf -i 1-30 -n 1) ; python /home/airflow/gcs/dags/edwra/scripts/{} -table {} -si {}'.format(config_ra_oracle['v_jdbcbqpytemplate_gcs'], table_id, src_schema_id)
                            )]
                            # Sensor that checks Dataflow job status every 5 minutes

                            wait_for_adhoc_pull_job_async_done = DataflowJobStatusSensor(
                                task_id="wait_for_adhoc_pull_job_async_done",
                                job_id=f"{{{{ task_instance.xcom_pull(task_ids= 'run_dataflow_adhoc_pull_jobs.TG-oracle_df_{table_id}_adhoc_pull.run_plus_oracle_{table_id}')}}}}",
                                expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
                                mode="reschedule",
                                poke_interval=300,
                                location='us-east4',
                            )
                            post_processing_adhoc = PythonOperator(
                                python_callable = cu.gcs_to_bq_load_adhoc,
                                task_id = f'oracle_post_processing_{table_id}',
                                op_kwargs = {"table_key": table_id.lower(), "schema_id": src_schema_id}
                            )
                        
                        run_dataflow_job_adhoc_pull >> wait_for_adhoc_pull_job_async_done >> post_processing_adhoc
                    [tablegroup_adhoc]


        if has_adhoc_pull is True:    
            start_job >> done_file_grp >> update_adhoc_pull_var >>  tg_adhoc_pull  >> end_job
        else:
            start_job >> done_file_grp >> update_adhoc_pull_var >>  tg_adhoc_pull >> end_job
        
        return dag

source_system = config_ra_oracle['v_sourcesysnm']
source_db = config_ra_oracle['v_databasetype']

# Extract DAG/Scheduling information from config file
for schedule in config_ra_oracle['schedule']:
    frequency = schedule["frequency"]
    done_files = schedule["done_files"]
    has_sensor = schedule["has_sensor"]
    sensor_list = schedule["sensor"] if has_sensor is True else None
    schedule_interval = schedule["v_schedule_interval"]
    start_date = schedule["start_date"]
    tblists = schedule["tblist"]
    has_adhoc_pull = schedule["has_adhoc_pull"]
    has_file_extract = schedule["has_file_extract"]
    if has_file_extract is True:
        source_file_name = schedule["source_file_name"]
        destination_file_name = schedule["destination_file_name"]
    else:
        source_file_name = None
        destination_file_name = None
    type = config_ra_oracle['v_databasetype']
    schema_list = schedule["schema_list"]

    if schedule_interval == "full_load":
        interval_range = "adhoc"
        scheduled_execution = None
    else:
        scheduled_execution = schedule_interval
        time = scheduled_execution.split(" ")
        interval_range = time[1].zfill(2) + "." + time[0].zfill(2)
    for schema in schema_list:
        if scheduled_execution:
            dag_id = '_'.join(['dag_ingest', source_system, schema, type, frequency, interval_range])
        else:
            dag_id = '_'.join(['dag_ingest', source_system, schema, type, frequency,schedule_interval])
        globals()[dag_id] = create_dag(dag_id, scheduled_execution, start_date, source_system, source_db, frequency, done_files, has_sensor, sensor_list, tblists, has_adhoc_pull, has_file_extract, source_file_name, destination_file_name, schema,schedule_interval)
