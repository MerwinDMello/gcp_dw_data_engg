from airflow import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.hooks.dataflow import DataflowJobStatus
from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatusSensor
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from datetime import datetime, timedelta
import os
import sys
import time
import pendulum
import logging


script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, '..', 'utils')
sys.path.append(util_dir)
import common_utilities as cu

lob = "edwra"
lob = lob.lower().strip()
lob_abbr = "ra"
src_system = "sqlserver"
src_system = src_system.lower().strip()
config = cu.call_config_yaml(f"{lob_abbr}_config.yaml", f"hca_{lob_abbr}_default_vars")
srcsys_config = cu.call_config_yaml(f"ra_{src_system}_ingest_config.yaml", f"ra_{src_system}_ingest")
timezone = pendulum.timezone("US/Central")
ra_config_file_name = "ra_config.yaml"

## function used to calculate a source from date if needed
# def calculate_v_from(**kwargs):
#     execution_date = kwargs['execution_date']
#     if execution_date.weekday() == 6:  # Sunday is 6
#         v_from = '1900-01-01'
#         print(f"Execution date: {execution_date}, v_from: {v_from}")
#         return v_from
#     else:
#         v_from = (execution_date - timedelta(days=5)).strftime("%Y-%m-%d")  # Subtract 5 days and format the date
#         print(f"Execution date: {execution_date}, v_from: {v_from}")
#         return v_from

default_args = {
    'owner': f'hca_ra_atos',
    'depends_on_past': False,
    'email': config['env']['v_failure_email_list'],
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=480),
    'params': {
        "param_parallon_ra_stage_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_parallon_ra_stage_dataset_name'],
        "param_parallon_ra_core_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_parallon_ra_core_dataset_name'],
        "param_parallon_ra_base_views_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_parallon_ra_base_views_dataset_name'],
        "param_parallon_ra_views_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_parallon_ra_views_dataset_name'],
        "param_bqutil_fns_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_bqutil_fns_dataset_name'],
        "param_auth_base_views_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_auth_base_views_dataset_name'],
        "param_parallon_ra_audit_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_parallon_ra_audit_dataset_name'],
        "param_parallon_cur_project_id": config['env']['v_curated_project_id'],
    }
}

def create_dag(dag_id, schedule, start_date, source_system, db_type, frequency, tags, done_file, has_sensor, sensor_list, tblist, truncatetablelist):
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        start_date=eval(start_date),
        schedule_interval=schedule,
        catchup=False,
        max_active_runs=1,
        max_active_tasks=10,
        concurrency=10,
        tags=tags,
        template_searchpath='/home/airflow/gcs/dags/edwra/sql/'
    )

    with dag:

        start_job = DummyOperator(task_id='start_dag')
        end_job = DummyOperator(task_id='end_dag')
        table_grp_arr = []

        if (done_file != 'NONE'):
            prev = ''
            now= ''
            now = pendulum.now(timezone).strftime("%Y%m%d")
            prev = (pendulum.now(timezone).subtract(days=1)).strftime("%Y%m%d")
            done_file_to_delete =  done_file.replace('YYYYMMDD', prev)     
            done_file= done_file.replace('YYYYMMDD', now)
            file_sensor = GCSObjectsWithPrefixExistenceSensor(
                    bucket=config['env']['v_data_bucket_name'],
                    prefix=config['env']['v_srcfilesdir']  + source + '/' + done_file,
                    timeout=18000,
                    mode="reschedule",
                    task_id=f"check_{source}_done_file_exists"
            )
            
            delete_old_done_files = PythonOperator(
                task_id=f'delete_{source_system}_old_done_files',
                python_callable=cu.removegcsfileifexists,
                op_kwargs={
                    'sourcesysname' : source,
                    'folder' : config['env']['v_srcfilesdir'],
                    'filename' : done_file_to_delete
                    }
            )
        if has_sensor:
            with TaskGroup(group_id=f'TG-sensor-{source}') as sensor_grp:
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
        # these are full table loads, so truncate before load.
        if len(truncatetablelist)!=0:
            with TaskGroup(group_id='truncate_tables') as tg1:
                truncate_task = DummyOperator(task_id=f'T-truncate-table') 
                truncate_arr = []
                for idx,tgttablename in enumerate(truncatetablelist):
                    truncate_table = BigQueryExecuteQueryOperator(
                        task_id="truncate_table_" + tgttablename,
                        dag=dag,
                        sql="truncate table " +
                            config['env'][f'v_parallon_{lob_abbr}_stage_dataset_name'] +
                                '.' + tgttablename,
                        use_legacy_sql=False,
                        gcp_conn_id=config['env']['v_curated_conn_id']
                    )
                    if idx == 0:
                        truncate_task >> truncate_table
                    else:
                        truncate_arr[-1] >> truncate_table
                    truncate_arr.append(truncate_table)

        #run parallel df jobs
        for ind,srvr in enumerate(server):
            server_name = srvr
            server_name_formatted = server_name.replace(".","_").lower()
            db_list = schema
            with TaskGroup(group_id=f'run_dataflow_jobs_{ind}') as dataflow_group:
                for list in tblist:
                    with TaskGroup(group_id=f'TG-sql_df_{list}') as tablegroup:

                        run_dataflow_job = [BashOperator(
                            task_id=f"run_{source_system}_{list}",
                            dag=dag,
                            # to start parallel tasks at random times within 5 minutes, sleep n seconds
                            bash_command=f"sleep $(shuf -i 1-300 -n 1) ; python /home/airflow/gcs/dags/edwra/scripts/{srcsys_config['v_jdbcbqpytemplate']}"
                            f" --src_sys_config_file={source_system}_sqlserver_ingest_config.yaml --src_tbl_list={list} --src_server_name={server_name}"
                            f" --src_system={source_system} --src_db_list=\'{db_list}\' --src_db_type={db_type}"
                            " --beam_runner=DataflowRunner"
                        )]

                        wait_for_python_job_async_done = DataflowJobStatusSensor(
                            task_id=f"wait_for_python_job_async_done_{list}",
                            job_id=f"{{{{ task_instance.xcom_pull(task_ids= 'run_dataflow_jobs_{ind}.TG-sql_df_{list}.run_{source_system}_{list}')}}}}",
                            expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
                            mode="reschedule",
                            poke_interval=300,
                            location=config['env']['v_region'],
                        )

                        run_dataflow_job >> wait_for_python_job_async_done
                    [tablegroup]

        if len(sqls)!=0:
            with TaskGroup(group_id=f'TG-update-denials') as sql_grp:
                sql_arr = []

                for idx, sqlname in enumerate(sqls):
                    update_bq_table = BigQueryOperator(
                        gcp_conn_id=config['env']['v_curated_conn_id'],
                        sql=f'dml/integration/incremental/{sqlname}',
                        use_legacy_sql=False,
                        retries=0,
                        task_id=f'run_{sqlname}'
                    )

                    if idx == 0:
                        [update_bq_table]
                    else:
                        sql_arr[-1] >> update_bq_table
                    sql_arr.append(update_bq_table)

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

        # run dag
        # separate dag task process for the denials update
        if 'denials-new' in tblist:
            start_job >> sensor_grp >> sql_grp >> dataflow_group >> end_job
        else:
            if has_file_extract is True:
                if has_sensor:
                    if (done_file != 'NONE'):
                        start_job >> file_sensor >> sensor_grp >> tg1 >> dataflow_group >> delete_old_done_files >> file_export_df_job >> wait_for_done_file_job_async_done >> end_job
                    elif (len(truncatetablelist)!=0):
                        start_job >> sensor_grp >>  tg1 >> dataflow_group >> file_export_df_job >> wait_for_done_file_job_async_done >> end_job
                    else:
                        start_job >> sensor_grp >> dataflow_group >> file_export_df_job >> wait_for_done_file_job_async_done >> end_job
                else:
                    if (done_file != 'NONE'):
                        start_job >> file_sensor  >> tg1 >> dataflow_group >> delete_old_done_files >> file_export_df_job >> wait_for_done_file_job_async_done >> end_job
                    elif (len(truncatetablelist)!=0):
                        start_job >> tg1 >> dataflow_group >> file_export_df_job >> wait_for_done_file_job_async_done >> end_job
                    else:
                        start_job >> dataflow_group >> file_export_df_job >> wait_for_done_file_job_async_done >> end_job
            else:
                if has_sensor:
                    if (done_file != 'NONE'):
                        start_job >> file_sensor >> sensor_grp >> tg1 >> dataflow_group >> delete_old_done_files >> end_job
                    elif (len(truncatetablelist)!=0):
                        start_job >> sensor_grp >>  tg1 >> dataflow_group >> end_job
                    else:
                        start_job >> sensor_grp >> dataflow_group >> end_job
                else:
                    if (done_file != 'NONE'):
                        start_job >> file_sensor  >> tg1 >> dataflow_group >> delete_old_done_files >> end_job
                    elif (len(truncatetablelist)!=0):
                        start_job >> tg1 >> dataflow_group >> end_job
                    else:
                        start_job >> dataflow_group >> end_job

source_system = srcsys_config['v_sourcesysnm']
db_type = srcsys_config['v_databasetype']
for schedule in srcsys_config['schedule']:
    frequency = schedule["frequency"]
    schedule_interval = schedule["schedule_interval"]
    start_date = schedule["start_date"]
    done_file = schedule["done_file"]
    has_sensor = True if schedule["has_sensor"] == "Yes" else False
    sensor_list = schedule["sensor"] if has_sensor else None
    tblist = schedule["tblist"]
    truncatetablelist=schedule["truncatetablelist"]
    server=schedule["server"]
    schema=schedule["schema"]
    source=schedule["source"]
    has_file_extract = schedule["has_file_extract"]
    if schedule["has_file_extract"] is True:
        source_file_name = schedule["source_file_name"]
        destination_file_name = schedule["destination_file_name"]
    else:
        source_file_name = None
        destination_file_name = None
    
    # grab update sqls only needed for denials
    if 'denials-new' in tblist:
        sqls = schedule['sqls']
    else:
        sqls = []

    if schedule_interval  == "None":
        interval_range =  "None"
        schedule = None
    else:
        schedule = schedule_interval
        time = schedule.split(" ")
        interval_range = time[1].zfill(2) + "."  + time[0].zfill(2)

    tags = [source_system]

    if frequency == "None":
        dag_id = f'dag_ingest_{source_system}_{db_type}_{source}_adhoc'
        tags.extend(['adhoc', 'ra'])
    else:
        dag_id = f'dag_ingest_{source_system}_{db_type}_{source}_{frequency}_{interval_range}'
        tags.extend([frequency, 'ra'])
    
    globals()[dag_id] = create_dag(dag_id, schedule, start_date, source_system, db_type, frequency, tags, done_file, has_sensor, sensor_list, tblist, truncatetablelist)