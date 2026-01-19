from airflow import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.email_operator import EmailOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from jinja2 import Template
# from google.cloud import bigquery
from google.cloud import storage
from google.cloud.storage.retry import DEFAULT_RETRY
from datetime import datetime, timedelta
import os
import sys
import time
import pendulum
import logging
import tempfile
import json

script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, '..', 'utils')
sys.path.append(util_dir)
import common_utilities as cu

audit_dir = os.path.join(script_dir, '..', 'sql', 'dml', 'audit_notification')

lob = "edwra"
lob = lob.lower().strip()
lob_abbr = "ra"
config = cu.call_config_yaml(f"{lob_abbr}_config.yaml", f"hca_{lob_abbr}_default_vars")
notification_config = cu.call_config_yaml(f"ra_notification_config.yaml", f"ra_notification")
timezone = pendulum.timezone("US/Central")

# Added for Retrying GCP APIs that fail due to timeout or failed health checks 
# Customize retry with a deadline of 180 seconds (default=120 seconds).
modified_retry = DEFAULT_RETRY.with_deadline(180)
# Customize retry with an initial wait time of 1.5 (default=1.0).
# Customize retry with a wait time multiplier per iteration of 1.2 (default=2.0).
# Customize retry with a maximum wait time of 15.0 (default=60.0).
modified_retry = modified_retry.with_delay(initial=1.5, multiplier=1.2, maximum=15.0)

bq_project_id = config['env']['v_curated_project_id']
environment = config['env']['v_env_name']
team_notification_email = config['env']['v_team_notification_email']
src_folder = config['env']['v_srcfilesdir']
stnd_gcs_bucket = config['env']['v_data_bucket_name']
notify_name = f'{lob_abbr.upper()} Team'

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

def read_template(util_folder, filename):
    template = open(os.path.join(util_folder, filename) , 'r')
    return template.read()

def render_template(content_template, dict_instance):
    template_instance = Template(content_template)
    rendered_template = template_instance.render(dict_instance)
    return rendered_template

def qualify_directory(directory_hierarchy, file_instance_dict):

    directory_path_words = directory_hierarchy.split("|")

    directory_path = directory_hierarchy.replace("|","/")

    if "SRC_FOLDER" in directory_path_words:
        directory_path = directory_path.replace("SRC_FOLDER", file_instance_dict['folder'])

    if "SRCARCHIVE_FOLDER" in directory_path_words:
        directory_path = directory_path.replace("SRCARCHIVE_FOLDER", file_instance_dict['folder'])

    if "SRC_SYSTEM" in directory_path_words:
        directory_path = directory_path.replace("SRC_SYSTEM", file_instance_dict['source_system'])

    if "PROCESSING_GROUP" in directory_path_words:
        directory_path = directory_path.replace("PROCESSING_GROUP", file_instance_dict['process_group'])

    if "FILE_EXTENSION" in directory_path_words:
        directory_path = directory_path.replace("FILE_EXTENSION", file_instance_dict['file_extension'])

    directory_path = directory_path.replace(chr(47)*2, chr(47)).lower()

    return directory_path

def qualify_file_pattern(file_pattern, file_instance_dict):

    file_name = file_pattern

    if "FILE_NAME" in file_name:
        file_name = file_name.replace("FILE_NAME", file_instance_dict['file_name_only'])

    if "FILE_PREFIX" in file_name:
        file_name = file_name.replace("FILE_PREFIX", file_instance_dict['file_prefix'])

    if "FILE_EXTENSION" in file_name:
        file_name = file_name.replace("FILE_EXTENSION", file_instance_dict['file_extension'])

    if "PROCESSING_GROUP" in file_name:
        file_name = file_name.replace("PROCESSING_GROUP", file_instance_dict['process_group'])

    return file_name

def get_bucket(bq_project_id, bucket_name):
    # Get name of bucket
    try:
        storage_client = storage.Client(bq_project_id)
        bucket = storage_client.bucket(bucket_name)
        return bucket
    except:
        logging.info("Bucket {} is not found in project {}".format(bucket_name, bq_project_id))
        raise SystemExit()

def get_blob(bucket, objectfullpath):
    # Get full path of the file
    try:
        
        blob = bucket.blob(objectfullpath)
        logging.info(f"Found Object {objectfullpath}")
        return blob
    except:
        logging.info("Blob is not found in path {}".format(objectfullpath))
        raise SystemExit()

def check_object_exists(bq_project_id, gcs_bucket, objectfullpath):
    # Check Object exists in the path of the bucket
    logging.info("==== In check_object_exists ====")
    logging.info("Project : {}, GCS Bucket : {}, Object Path : {}".format(
        bq_project_id, gcs_bucket, objectfullpath))

    storage_client = storage.Client(bq_project_id)
    bucket = storage_client.bucket(gcs_bucket)
    blob_exists = storage.Blob(bucket=bucket, name=objectfullpath).exists(storage_client)
    return blob_exists

def download_object_to_file(bq_project_id, gcs_bucket, objectfullpath, filepath):
    # Download gcs bucket object to local runtime directory
    logging.info("===== In download_object_to_file =====")
    logging.info("Project : {}, GCS Bucket : {}, Object Path : {}, File Path : {}".format(
        bq_project_id, gcs_bucket, objectfullpath, filepath))

    bucket = get_bucket(bq_project_id, gcs_bucket)
    blob_exists = check_object_exists(bq_project_id, gcs_bucket, objectfullpath)
    if not blob_exists:
        logging.error("File is not present in GCS")
        return None
    blob = get_blob(bucket, objectfullpath)
    blob.download_to_filename(filepath, retry=modified_retry)
    if os.path.getsize(filepath) == 0:
        logging.error("Empty File, exiting")
        return None
    
    filelocalname = os.path.abspath(filepath)
    logging.info("Downloaded file {}. ".format(filelocalname))
    return filelocalname

def get_outbound_files(**kwargs):
    ti = kwargs['ti']

    process_groups = kwargs['process_groups']
    file_extension = kwargs['file_extension']
    file_patterns = kwargs['file_patterns']
    directory_hierarchy = kwargs['directory_hierarchy']
    temp_dir = tempfile.gettempdir()
    local_file_list = []

    for process_group in process_groups:
        logging.info('Processing Group : {}'.format(process_group))
        file_instance_dict = {}
        file_instance_dict['folder'] = src_folder
        file_instance_dict['source_system'] = source_system
        file_instance_dict['file_extension'] = file_extension
        file_instance_dict['process_group'] = process_group
        input_directory_path = qualify_directory(directory_hierarchy, file_instance_dict)
        logging.info('Input Directory : {}'.format(input_directory_path))
        for file_pattern in file_patterns:
            input_file_name = qualify_file_pattern(file_pattern, file_instance_dict)
            logging.info('Input File : {}'.format(input_file_name))
            objectfullpath = '{}{}'.format(input_directory_path, input_file_name)
            file_location = os.path.join(temp_dir, input_file_name)
            filelocalname = download_object_to_file(bq_project_id, stnd_gcs_bucket, objectfullpath, file_location)
            logging.info('Local File : {}'.format(filelocalname))

            if filelocalname is not None:
                local_file_list.append(filelocalname)

    ti.xcom_push(key='file_list', value=local_file_list)

    return None


def exec_bq_query(**kwargs):
    ti = kwargs['ti']
    sql_script_file_name = kwargs['sql_script_file_name']

    sql_template = read_template(audit_dir, sql_script_file_name)
    sql_dict = {
    "params":params
    }
    sql_script = render_template(sql_template, sql_dict)

    # bq_client = bigquery.Client()
    # bq_resultset = bq_client.query(bq_query, project=bq_project_id, location='US').result()

    # target_rec_list = []
    # for bq_row in bq_resultset:
    #     rec_dict_item = {"Stack":bq_row.Stack, "Rec_Count":bq_row.Rec_Count}
    #     target_rec_list.append(rec_dict_item)

    hook = BigQueryHook(gcp_conn_id=config['env']['v_curated_conn_id'])
    target_df = hook.get_pandas_df(sql=sql_script, dialect='standard')

    ti.xcom_push(key='rec_list', value=json.loads(target_df.to_json(orient='records')))
    return None

def build_email_content(**kwargs):
    ti = kwargs['ti']
    notification_type = kwargs['notification_type']
    html_template_file_name = kwargs['html_template_file_name']
    file_extension = kwargs['file_extension']

    html_dict = {
    "notify_name":notify_name
    }
    
    if file_extension:
        target_file_list = ti.xcom_pull(task_ids=f'run_notification_{notification_type}.download_outbound_files', key='file_list')
        html_dict.update({"file_list": target_file_list})
    else:
        target_rec_list = ti.xcom_pull(task_ids=f'run_notification_{notification_type}.run_bq_query', key='rec_list')
        html_dict.update({"rec_list": target_rec_list})
        target_file_list = []

    content_template = read_template(util_dir, html_template_file_name)
    logging.info(f"Content {content_template}")
    logging.info(f"Dictionary {html_dict}")
    html_content = render_template(content_template, html_dict)

    ti.xcom_push(key='html_content', value=html_content)
    ti.xcom_push(key='file_list', value=target_file_list)
    return None

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
    'params': params
}

def create_dag(dag_id, schedule, source_system, start_date, tags, notification_process):
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
        template_searchpath='/home/airflow/gcs/dags/edwra/sql/',
        render_template_as_native_obj=True
    )

    with dag:

        start_job = DummyOperator(task_id='start_dag')
        end_job = DummyOperator(task_id='end_dag')

        done_file = notification_process["done_file"].upper()
        has_sensor = True if notification_process["has_sensor"] == "Yes" else False
        sensor_list = notification_process["sensor"] if has_sensor else None
        trigger_dag_ids = notification_process["trigger_dag_ids"]

        if (done_file != 'NONE'):
            prev = ''
            now= ''
            now = pendulum.now(timezone).strftime("%Y%m%d")
            prev = (pendulum.now(timezone).subtract(days=1)).strftime("%Y%m%d")
            done_file_to_delete =  done_file.replace('YYYYMMDD', prev)     
            done_file= done_file.replace('YYYYMMDD', now)
            file_sensor = GCSObjectsWithPrefixExistenceSensor(
                    bucket=config['env']['v_data_bucket_name'],
                    prefix=config['env']['v_srcfilesdir']  + db_type + '/' + done_file,
                    timeout=18000,
                    mode="reschedule",
                    task_id=f"check_{db_type}_done_file_exists"
            )
            
            delete_old_done_files = PythonOperator(
                task_id=f'delete_{db_type}_old_done_files',
                python_callable=cu.removegcsfileifexists,
                op_kwargs={
                    'sourcesysname' : db_type,
                    'folder' : config['env']['v_srcfilesdir'],
                    'filename' : done_file_to_delete
                    }
            )
        if has_sensor:
            with TaskGroup(group_id=f'TG-sensor-{source_system}') as sensor_grp:
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

        notifications = notification_process["notifications"]

        for notification in notifications:
            notification_type = notification["notification_type"]

            if environment.lower() != "prod":
                subject = f"{environment.upper()} - {notification['subject']}"
            else:
                subject = notification["subject"]

            html_template_file_name = notification["html_template_file_name"]

            if "process_groups" in notification:
                process_groups = notification["process_groups"]
            else:
                process_groups = None

            if "file_extension" in notification:
                file_extension = notification["file_extension"]
            else:
                file_extension = None

            if "file_patterns" in notification:
                file_patterns = notification["file_patterns"]
            else:
                file_patterns = None

            if "directory_hierarchy" in notification:
                directory_hierarchy = notification["directory_hierarchy"]
            else:
                directory_hierarchy = None

            if  "sql_script_file_name" in notification:
                sql_script_file_name = notification["sql_script_file_name"]
            else:
                sql_script_file_name = None

            with TaskGroup(group_id=f'run_notification_{notification_type}') as notification_group:

                if file_extension:
                    download_outbound_files = PythonOperator(
                        task_id=f'download_outbound_files',
                        python_callable=get_outbound_files,
                        provide_context=True,
                        op_kwargs={
                            'process_groups' : process_groups,
                            'file_extension' : file_extension,
                            'file_patterns' : file_patterns,
                            'directory_hierarchy' : directory_hierarchy
                        }
                    )
                else:
                    run_bq_query = PythonOperator(
                        task_id=f'run_bq_query',
                        python_callable=exec_bq_query,
                        provide_context=True,
                        op_kwargs={
                            'sql_script_file_name' : sql_script_file_name
                        }
                    )

                render_email_content = PythonOperator(
                    task_id=f'render_email_content',
                    python_callable=build_email_content,
                    provide_context=True,
                    op_kwargs={
                        'html_template_file_name' : html_template_file_name,
                        'notification_type' : notification_type,
                        'file_extension' : file_extension
                    }
                )

                send_email_task = EmailOperator(
                    task_id='send_email_task',
                    to=team_notification_email,
                    subject=subject,
                    html_content=f"{{{{ ti.xcom_pull(task_ids='run_notification_{notification_type}.render_email_content', key='html_content') }}}}",
                    files=f"{{{{ ti.xcom_pull(task_ids='run_notification_{notification_type}.render_email_content', key='file_list') }}}}",
                    dag=dag
                    ,retries=3
                )

                if file_extension:
                    download_outbound_files >> render_email_content >> send_email_task
                else:
                    run_bq_query >> render_email_content >> send_email_task

        # Trigger the dependent DAGs passed from the ingest configuration file
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
            if has_sensor:
                if (done_file != 'NONE'):
                    start_job >> file_sensor >> sensor_grp >> notification_group >> delete_old_done_files >> trigger_dag_grp >> end_job
                else:
                    start_job >> sensor_grp >> notification_group >> trigger_dag_grp >> end_job
            else:
                if (done_file != 'NONE'):
                    start_job >> file_sensor >> notification_group >> delete_old_done_files >> trigger_dag_grp >> end_job
                else:
                    start_job >> notification_group >> trigger_dag_grp >> end_job
        else:
            if has_sensor:
                if (done_file != 'NONE'):
                    start_job >> file_sensor >> sensor_grp >> notification_group >> delete_old_done_files >> end_job
                else:
                    start_job >> sensor_grp >> notification_group >> end_job
            else:
                if (done_file != 'NONE'):
                    start_job >> file_sensor >> notification_group >> delete_old_done_files >> end_job
                else:
                    start_job >> notification_group >> end_job

for notification_process in notification_config['schedule']:
    source_system = notification_process['source_name']
    db_type = notification_process['database_type']
    frequency = notification_process["frequency"]
    schedule_interval = notification_process["schedule_interval"]
    if schedule_interval  == "None":
        schedule = None
    else:
        schedule = schedule_interval
    start_date = notification_process["start_date"]
    dag_name_suffix = notification_process["dag_name_suffix"]
    
    tags = [source_system, dag_name_suffix]

    if frequency == "None":
        dag_id = f'dag_notification_{source_system}_{db_type}_adhoc_{dag_name_suffix}'
        tags.extend(['adhoc', 'ra'])
    else:
        dag_id = f'dag_notification_{source_system}_{db_type}_{frequency}_{dag_name_suffix}'
        tags.extend([frequency, 'ra'])
    
    globals()[dag_id] = create_dag(dag_id, schedule, source_system, start_date, tags, notification_process)