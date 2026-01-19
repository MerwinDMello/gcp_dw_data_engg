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
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from jinja2 import Template
from datetime import datetime, timedelta
import os
import sys
import time
import pendulum
import logging

script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, '..', 'utils')
audit_dir = os.path.join(script_dir, '..', 'sql', 'dml', 'audit_notification')
sys.path.append(util_dir)
import common_utilities as cu

lob = "edwra"
lob = lob.lower().strip()
lob_abbr = lob.replace("edw","")
source_system = "govreports"
source_system = source_system.lower().strip()
config = cu.call_config_yaml(f"{lob_abbr}_config.yaml", f"hca_{lob_abbr}_default_vars")
config_type = "ingest"
gcp_conn_id = config['env']['v_curated_conn_id']
bq_project_id = config['env']['v_curated_project_id']
df_template = config['env']['v_file_df_template']
ingest_config = cu.call_config_yaml(f"ra_{source_system}_{config_type}_config.yaml", f"{lob_abbr}_{source_system}_{config_type}")
timezone = pendulum.timezone("US/Central")

params = {
        "param_parallon_ra_stage_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_parallon_ra_stage_dataset_name'],
        "param_parallon_ra_core_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_parallon_ra_core_dataset_name'],
        "param_parallon_ra_base_views_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_parallon_ra_base_views_dataset_name'],
        "param_parallon_ra_views_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_parallon_ra_views_dataset_name'],
        "param_bqutil_fns_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_bqutil_fns_dataset_name'],
        "param_auth_base_views_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_auth_base_views_dataset_name'],
        "param_parallon_ra_audit_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_parallon_ra_audit_dataset_name'],
        "param_parallon_cur_project_id": config['env']['v_curated_project_id'],
    }

default_args = {
    'owner': f'hca_{lob_abbr}_atos',
    'depends_on_past': False,
    'email': config['env']['v_failure_email_list'],
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=480),
    'params': params
}

def read_template(util_folder, filename):
    template = open(os.path.join(util_folder, filename) , 'r')
    return template.read()

def render_template(content_template, dict_instance):
    template_instance = Template(content_template)
    rendered_template = template_instance.render(dict_instance)
    return rendered_template

def exec_bq_query(sql_script_file_name, process_group):

    sql_template = read_template(audit_dir, sql_script_file_name)
    sql_dict = {
    "params":params,
    "operational_group":process_group
    }
    sql_script = render_template(sql_template, sql_dict)
    logging.info(sql_script)
    return sql_script

def success_fn(first_cell):
    print(f"Value of first_cell: {first_cell} and Type : {type(first_cell)}")
    return not first_cell

def failure_fn(first_cell):
    print(f"Value of first_cell: {first_cell} and Type : {type(first_cell)}")
    return first_cell

def create_dag(dag_id, schedule, start_date, dag_name_suffix, source_system, frequency, tags, done_file, truncatetablelist, has_sensor, sensor_list, trigger_dag_ids, process_groups, ingest_process):
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

        # these are full table loads, so truncate before load.
        if truncatetablelist:
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

        if "get_files" in ingest_process:
            get_files = ingest_process["get_files"]
        else:
            get_files = None

        if "convert_files" in ingest_process:
            convert_files = ingest_process["convert_files"]
        else:
            convert_files = None
        
        if "load_files" in ingest_process:
            load_files = ingest_process["load_files"]
        else:
            load_files = None
        
        if "data_checks_sql_list" in ingest_process:
            data_checks_sql_list = ingest_process["data_checks_sql_list"]
        else:
            data_checks_sql_list = None

        if data_checks_sql_list:
            pre_data_checks_sql_list = list(filter(lambda d: d['processing_stage'].lower() == 'pre', data_checks_sql_list))
            logging.info(pre_data_checks_sql_list)
            post_data_checks_sql_list = list(filter(lambda d: d['processing_stage'].lower() == 'post', data_checks_sql_list))
            logging.info(post_data_checks_sql_list)
        else:
            pre_data_checks_sql_list = None
            post_data_checks_sql_list = None

        with TaskGroup(group_id=f'TG-source_{source_system.lower()}') as source_group:
            for process_group in process_groups:
                with TaskGroup(group_id=f'TG-ssc_{process_group.lower()}') as process_df_group:
                    sub_task_group = None

                    # Execute Queries after the files are copied
                    if pre_data_checks_sql_list:
                        with TaskGroup(group_id=f'run_pre_sql_checks') as check_pre_sql_group:
                            for pre_data_checks_sql in pre_data_checks_sql_list:
                                sql_script_file_name = pre_data_checks_sql["sql_template_file_name"]
                                sql_sensor_task = SqlSensor(
                                    task_id=f'sql_check_for_{process_group}_{sql_script_file_name.split(".")[0]}',
                                    conn_id="gcpbigquery",
                                    hook_params = {"location":"US", "use_legacy_sql":False},
                                    success=success_fn,
                                    failure=failure_fn,
                                    sql=exec_bq_query(sql_script_file_name, process_group),
                                    dag=dag)
                                
                                sql_sensor_task
                            if sub_task_group != None:
                                sub_task_group.set_downstream(check_pre_sql_group)
                            sub_task_group = check_pre_sql_group

                    #run parallel df jobs for getting files
                    if get_files:
                        file_server_list = ingest_process["file_server_info"]
                        with TaskGroup(group_id=f'run_get_files_dataflow_jobs') as get_files_dataflow_group:
                            for file_server in file_server_list:                
                                server_name = file_server["file_server"]
                                server_name_formatted = server_name.replace(".","_").lower()
                                with TaskGroup(group_id=f'run_server_{server_name_formatted}') as server_group:
                                    for get_file in get_files:
                                        file_name_mnemonic = get_file["file_name_mnemonic"]
                                        run_get_files_dataflow_job = [BashOperator(
                                            task_id=f"run_get_files_{file_name_mnemonic.lower()}",
                                            dag=dag,
                                            # to start parallel tasks at random times within 5 minutes, sleep n seconds
                                            bash_command=f"sleep $(shuf -i 1-30 -n 1) ; python /home/airflow/gcs/dags/{lob}/scripts/{df_template}"
                                            f" --src_system={source_system} --dag_name_suffix={dag_name_suffix} --beam_runner=DataflowRunner"
                                            f" --file_name_mnemonic={file_name_mnemonic} --process_type=Get_Files --config_type={config_type}"
                                            f" --process_group={process_group} --server={server_name}"
                                        )]

                                        wait_for_python_get_files_job_async_done = DataflowJobStatusSensor(
                                            task_id=f"wait_for_python_get_job_async_done_{file_name_mnemonic.lower()}",
                                            job_id=f"{{{{ task_instance.xcom_pull(task_ids= 'TG-source_{source_system.lower()}.TG-ssc_{process_group.lower()}.run_get_files_dataflow_jobs.run_server_{server_name_formatted}.run_get_files_{file_name_mnemonic.lower()}')}}}}",
                                            expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
                                            mode="reschedule",
                                            poke_interval=120,
                                            location=config['env']['v_region'],
                                        )
                                    
                                        run_get_files_dataflow_job >> wait_for_python_get_files_job_async_done
                                    [server_group]
                            if sub_task_group != None:
                                sub_task_group.set_downstream(get_files_dataflow_group)
                            sub_task_group = get_files_dataflow_group

                    #run parallel df jobs for converting files
                    if convert_files:
                        with TaskGroup(group_id=f'run_convert_files_dataflow_jobs') as convert_files_dataflow_group:
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
                                    job_id=f"{{{{ task_instance.xcom_pull(task_ids= 'TG-source_{source_system.lower()}.TG-ssc_{process_group.lower()}.run_convert_files_dataflow_jobs.run_convert_files_{file_name_mnemonic.lower()}')}}}}",
                                    expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
                                    mode="reschedule",
                                    poke_interval=120,
                                    location=config['env']['v_region'],
                                )
                            
                                run_convert_files_dataflow_job >> wait_for_python_convert_files_job_async_done
                            if sub_task_group != None:
                                sub_task_group.set_downstream(convert_files_dataflow_group)
                            sub_task_group = convert_files_dataflow_group

                    #run parallel df jobs for loading files
                    if load_files:
                        with TaskGroup(group_id=f'run_load_files_dataflow_jobs') as load_files_dataflow_group:
                            for load_file in load_files:
                                file_name_mnemonic = load_file["file_name_mnemonic"]

                                run_load_files_dataflow_job = [BashOperator(
                                    task_id=f"run_load_files_{file_name_mnemonic.lower()}",
                                    dag=dag,
                                    # to start parallel tasks at random times within 5 minutes, sleep n seconds
                                    bash_command=f"sleep $(shuf -i 1-30 -n 1) ; python /home/airflow/gcs/dags/{lob}/scripts/{df_template}"
                                    f" --src_system={source_system} --dag_name_suffix={dag_name_suffix} --beam_runner=DataflowRunner"
                                    f" --file_name_mnemonic={file_name_mnemonic} --process_type=Load_Files --config_type={config_type}"
                                    f" --process_group={process_group}"
                                )]

                                wait_for_python_load_files_job_async_done = DataflowJobStatusSensor(
                                    task_id=f"wait_for_python_load_job_async_done_{file_name_mnemonic.lower()}",
                                    job_id=f"{{{{ task_instance.xcom_pull(task_ids= 'TG-source_{source_system.lower()}.TG-ssc_{process_group.lower()}.run_load_files_dataflow_jobs.run_load_files_{file_name_mnemonic.lower()}')}}}}",
                                    expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
                                    mode="reschedule",
                                    poke_interval=120,
                                    location=config['env']['v_region'],
                                )
                            
                                run_load_files_dataflow_job >> wait_for_python_load_files_job_async_done
                            if sub_task_group != None:
                                sub_task_group.set_downstream(load_files_dataflow_group)
                            sub_task_group = load_files_dataflow_group

                    # Execute Queries after the files are loaded
                    if post_data_checks_sql_list:
                        with TaskGroup(group_id=f'run_post_sql_checks') as check_post_sql_group:
                            for post_data_checks_sql in post_data_checks_sql_list:
                                sql_script_file_name = post_data_checks_sql["sql_template_file_name"]
                                sql_sensor_task = SqlSensor(
                                    task_id=f'sql_check_for_{process_group}_{sql_script_file_name.split(".")[0]}',
                                    conn_id="gcpbigquery",
                                    hook_params = {"location":"US", "use_legacy_sql":False},
                                    success=success_fn,
                                    failure=failure_fn,
                                    sql=exec_bq_query(sql_script_file_name, process_group),
                                    dag=dag)
                                
                                sql_sensor_task
                            if sub_task_group != None:
                                sub_task_group.set_downstream(check_post_sql_group)
                            sub_task_group = check_post_sql_group             

                    [process_df_group]

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

        if truncatetablelist:
            last_task.set_downstream(truncate_grp)
            last_task = truncate_grp

        last_task.set_downstream(source_group)
        last_task = source_group

        if (done_file != 'NONE'):
            last_task.set_downstream(delete_old_done_files)
            last_task = delete_old_done_files

        if trigger_dag_ids:
            last_task.set_downstream(trigger_dag_grp)
            last_task = trigger_dag_grp

        last_task.set_downstream(end_job)

for ingest_process in ingest_config[source_system]:
    start_date = ingest_process["start_date"]
    schedule_interval = ingest_process["schedule"]
    frequency = ingest_process["frequency"]
    db_type = ingest_process['datasourcetype']
    dag_name_suffix = ingest_process["dag_name_suffix"]
    process_groups = ingest_process["process_groups"]
    truncatetablelist = ingest_process["truncatetablelist"]
    trigger_dag_ids = ingest_process["trigger_dag_ids"]
    done_file = ingest_process["done_file"]
    has_sensor = ingest_process["has_sensor"]
    if "sensor" in ingest_process:
        sensor_list = ingest_process["sensor"]
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
    
    globals()[dag_id] = create_dag(dag_id, schedule, start_date, dag_name_suffix, source_system, frequency, tags, done_file, truncatetablelist, has_sensor, sensor_list, trigger_dag_ids, process_groups, ingest_process)