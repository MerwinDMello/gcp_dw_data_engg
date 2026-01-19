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
ingest_config_name = 'ra_oracle_ingest_dependency_daily.yaml'
# Get source-specific config file
config_ra_oracle = cu.call_config_yaml(f"{ingest_config_name}","ra_oracle_ingest_dependency_daily")
timezone = pendulum.timezone("US/Central")

full_pull_table_var = "ra_full_pull_tables"

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


def create_dag(dag_id, schedule, start_date, source_system, source_db, frequency, done_files, has_sensor, sensor_list, tblists, has_full_pull, has_file_extract, source_file_name, destination_file_name, schema):
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
                    # [file_sensor]
        # If the job is dependent on another DAG, it waits for successful completion of that DAG
        if has_sensor is True:
            with TaskGroup(group_id=f'TG-sensor-{source_db}') as sensor_grp:
                # If there are multiple prerequisite DAGs it will create a task for each one
                for sensor in sensor_list:
                    ext_dag_id = sensor["dag_id"]
                    ext_task_id = sensor["task_id"]
                    schedule = sensor["schedule"]
                    if "cycle_age" in sensor:
                        cycle_age = sensor["cycle_age"]
                    else:
                        cycle_age = "current"
                    # Check status of DAG, only looks for successful run at specified time
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
        
        # Update Airflow variable that defines which tables are full pulls so that it can be used elsewhere in script
        if has_full_pull is True:
            update_full_pull_var = PythonOperator(
                task_id='update_full_pull_tables',
                python_callable=cu.update_cdc_airflow_var,
                op_kwargs = {"airflow_var_name": full_pull_table_var}
            )
        # Send done files to ETL server if needed for downstream on-prem jobs
        if has_file_extract is True:
            destination_file_name = destination_file_name.replace('$SCHEMA', str(schema).upper())
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

        potential_full_pull_tables:list = Variable.get(full_pull_table_var, default_var = [], deserialize_json=True)
        if potential_full_pull_tables == []:
            cu.update_cdc_airflow_var(airflow_var_name=full_pull_table_var)
        potential_full_pull_tables = [x for x in potential_full_pull_tables if x['schema_id'] == src_schema_id]
        # Tables need to be sorted to keep same order when tasks are dynamically created
        potential_full_pull_tables.sort(key=lambda x: x['table_id'])
        # These full pull table ids will be passed into normal daily dataflow jobs so that they are ignored
        full_pull_table_ids = [x['table_id'].lower() for x in potential_full_pull_tables if x['is_full_pull'] is True]

        with TaskGroup(group_id='run_dataflow_jobs') as tg:
            # Trigger dataflow job for each tblist, will run parallely
            for tblist in tblists:
                with TaskGroup(group_id=f'TG-oracle_df_{tblist}') as tablegroup:
                    # Trigger dataflow job
                    run_dataflow_job = [BashOperator(
                        task_id=f"run_plus_oracle_{tblist}",
                        dag=dag,
                        bash_command='sleep $(shuf -i 1-30 -n 1) ; python /home/airflow/gcs/dags/edwra/scripts/{} --src_sys_config_file={} --src_sys_airflow_varname=ra_oracle_ingest_dependency_daily --src_tbl_list={} --src_schema_id={} --tables_to_ignore {}'.format(
                            config_ra_oracle['v_jdbcbqpytemplate'], ingest_config_name, tblist, src_schema_id, ' '.join(full_pull_table_ids))
                    )]
                    # Sensor that checks Dataflow job status every 5 minutes
                    # There are options to continuously check for status but this way clears up slots
                    wait_for_python_job_async_done = DataflowJobStatusSensor(
                        task_id="wait_for_python_job_async_done",
                        job_id=f"{{{{ task_instance.xcom_pull(task_ids= 'run_dataflow_jobs.TG-oracle_df_{tblist}.run_plus_oracle_{tblist}')}}}}",
                        expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
                        mode="reschedule",
                        poke_interval=300,
                        location='us-east4',
                    )
                    post_processing = PythonOperator(
                        python_callable = cu.oracle_ingest_post_processing,
                        task_id = f'oracle_post_processing_{tblist}',
                        op_kwargs = {"table_infos": config_ra_oracle[tblist], "src_schema_id": src_schema_id, "tables_to_ignore": full_pull_table_ids}
                    )
                    run_dataflow_job >> wait_for_python_job_async_done >> post_processing              
            [tablegroup]
        if has_full_pull is True:
            with TaskGroup(group_id='run_dataflow_full_pull_jobs') as tg_full_pull:
                for table_info in potential_full_pull_tables:
                    table_id = table_info['table_id']
                    is_full_pull = table_info['is_full_pull']
                    with TaskGroup(group_id=f'TG-oracle_df_{table_id}_full_pull') as tablegroup_full:
                        # If a table is a full pull, run a different Dataflow script to more efficiently pull and load the data.
                        # Any tables that have full pulls done will be ignored in normal daily jobs
                        if is_full_pull is True:
                            run_dataflow_job_full_pull = [BashOperator(
                                task_id=f"run_plus_oracle_{table_id}",
                                dag=dag,
                                bash_command='sleep $(shuf -i 1-30 -n 1) ; python /home/airflow/gcs/dags/edwra/scripts/{} -table {} -si {}'.format(config_ra_oracle['v_jdbcbqpytemplate_gcs'], table_id, src_schema_id)
                            )]
                            # Sensor that checks Dataflow job status every 5 minutes
                            # There are options to continuously check for status but this way clears up slots
                            wait_for_full_pull_job_async_done = DataflowJobStatusSensor(
                                task_id="wait_for_full_pull_job_async_done",
                                job_id=f"{{{{ task_instance.xcom_pull(task_ids= 'run_dataflow_full_pull_jobs.TG-oracle_df_{table_id}_full_pull.run_plus_oracle_{table_id}')}}}}",
                                expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
                                mode="reschedule",
                                poke_interval=300,
                                location='us-east4',
                            )
                            post_processing_full = PythonOperator(
                                python_callable = cu.gcs_to_bq_load,
                                task_id = f'oracle_post_processing_{table_id}',
                                op_kwargs = {"table_key": table_id.lower(), "schema_id": src_schema_id}
                            )
                        # If a table is not a full pull. These jobs should not run, so we put in dummy tasks to not lose history in the Airflow UI
                        else:
                            run_dataflow_job_full_pull = [BashOperator(
                                task_id=f"run_plus_oracle_{table_id}",
                                dag=dag,
                                bash_command=f'echo "Table {table_id} is not a full pull today, no dataflow job triggered. Will run as part of normal ingest jobs"'
                            )]
                            wait_for_full_pull_job_async_done = BashOperator(
                                task_id="wait_for_full_pull_job_async_done",
                                dag=dag,
                                bash_command=f'echo "Table {table_id} is not a full pull today, no dataflow job triggered, so no dataflow job to check for"'
                            )
                            post_processing_full = BashOperator(
                                task_id = f'oracle_post_processing_{table_id}',
                                dag=dag,
                                bash_command=f'echo "Table {table_id} is not a full pull today, no dataflow job triggered, so no post processing to be done"'
                            )

                        run_dataflow_job_full_pull >> wait_for_full_pull_job_async_done >> post_processing_full
                [tablegroup_full]
        # Define task dependency relationships based on which dependencies the job has
        # >> symbol defines dependencies. (A >> B means task A will run and complete before task B begins)
        if has_full_pull is True:
            if done_files != 'None':
                if has_sensor:
                    if has_file_extract is True:
                        start_job >> done_file_grp >> sensor_grp >> update_full_pull_var >> [tg, tg_full_pull] >> file_export_df_job >> wait_for_done_file_job_async_done >> end_job
                    else:
                        start_job >> done_file_grp >> sensor_grp >> update_full_pull_var >> [tg, tg_full_pull] >> end_job
                else:
                    if has_file_extract is True:
                        start_job >> done_file_grp >> update_full_pull_var >> [tg, tg_full_pull] >> file_export_df_job >> wait_for_done_file_job_async_done >> end_job
                    else:
                        start_job >> done_file_grp >> update_full_pull_var >> [tg, tg_full_pull] >> end_job
            else:
                if has_sensor:
                    if has_file_extract is True:
                        start_job >> sensor_grp >> update_full_pull_var >> [tg, tg_full_pull] >> file_export_df_job >> wait_for_done_file_job_async_done >> end_job
                    else:
                        start_job >> sensor_grp >> update_full_pull_var >> [tg, tg_full_pull] >> end_job
                else:
                    if has_file_extract is True:
                        start_job >> update_full_pull_var >> [tg, tg_full_pull] >> file_export_df_job >> wait_for_done_file_job_async_done >> end_job
                    else:
                        start_job >> update_full_pull_var >> [tg, tg_full_pull] >> end_job
        else:
            if done_files != 'None':
                if has_sensor:
                    if has_file_extract is True:
                        start_job >> done_file_grp >> sensor_grp >> tg >> file_export_df_job >> wait_for_done_file_job_async_done >> end_job
                    else:
                        start_job >> done_file_grp >> sensor_grp >> tg >> end_job
                else:
                    if has_file_extract is True:
                        start_job >> done_file_grp >> tg >> file_export_df_job >> wait_for_done_file_job_async_done >> end_job
                    else:
                        start_job >> done_file_grp >> tg >> end_job
            else:
                if has_sensor:
                    if has_file_extract is True:
                        start_job >> sensor_grp >> tg >> file_export_df_job >> wait_for_done_file_job_async_done >> end_job
                    else:
                        start_job >> sensor_grp >> tg >> end_job
                else:
                    if has_file_extract is True:
                        start_job >> tg >> file_export_df_job >> wait_for_done_file_job_async_done >> end_job
                    else:
                        start_job >> tg >> end_job
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
    has_full_pull = schedule["has_full_pull"]
    has_file_extract = schedule["has_file_extract"]
    if has_file_extract is True:
        source_file_name = schedule["source_file_name"]
        destination_file_name = schedule["destination_file_name"]
    else:
        source_file_name = None
        destination_file_name = None
    type = config_ra_oracle['v_databasetype']
    schema_list = schedule["schema_list"]

    if schedule_interval == "None":
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
            dag_id = '_'.join(['dag_ingest', source_system, schema, type, frequency])
        globals()[dag_id] = create_dag(dag_id, scheduled_execution, start_date, source_system, source_db, frequency, done_files, has_sensor, sensor_list, tblists, has_full_pull, has_file_extract, source_file_name, destination_file_name, schema)
