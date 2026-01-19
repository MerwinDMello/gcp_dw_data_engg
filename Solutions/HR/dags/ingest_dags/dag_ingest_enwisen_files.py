from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.gcs_to_bq import GCSToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.bash_operator import BashOperator

from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup

from datetime import timedelta
import pendulum
import os
import sys

script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, '..', 'utils')
sys.path.append(util_dir)
# import common_utilities after setting the path for utils
import common_utilities as cu

current_timezone = pendulum.timezone("US/Central")

config = cu.call_config_yaml("hrg_config.yaml","hca_hrg_default_vars")
sftp_pgp_config = cu.call_config_yaml("enwisen_sftp_pgp_config.yaml", "enwisen_sftp_pgp_config_vars")
# get project variables from config file
env = config['env']['v_env_name']
bq_project_id = config['env']['v_curated_project_id']
bq_staging_dataset = config['env']['v_hr_stage_dataset_name']
gcs_bucket = config['env']['v_data_bucket_name']
src_folder = config['env']['v_srcfilesdir']
arc_folder = config['env']['v_archivedir']
tmp_folder = config['env']['v_tmpobjdir']
schema_folder = tmp_folder + config['env']['v_schemasubdir']
load_start_time = pendulum.now('US/Central').to_datetime_string()
schedule = sftp_pgp_config['sftp_pgp_settings']['v_sftp_del_file_schedule']
directory = sftp_pgp_config['sftp_pgp_settings']["v_directory"]
host = sftp_pgp_config['sftp_pgp_settings']['v_host']
user = sftp_pgp_config['sftp_pgp_settings']['v_user']
pwd_secret = config['env']['v_pwd_secrets_url'] + sftp_pgp_config['sftp_pgp_settings']['v_pwd_secret']


bq_src_project_id = config['env']['v_landing_project_id']
src_bucket = config['env']['v_data_bucket_name']
bq_tgt_project_id = config['low_env']['v_landing_project_id']
tgt_bucket = config['low_env']['v_data_bucket_name']

environment = config['env']['v_env_name']
lower_environment = config['low_env']['v_env_name']



default_args = {
    'owner': 'hca_hrg_atos',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2022, 12, 6, tz=current_timezone),
    'email': config['env']['v_failure_email_list'],
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=240),
    #params to substitute in sql files
    'params': {
        "param_hr_stage_dataset_name": bq_project_id + '.' +  config['env']['v_hr_stage_dataset_name'],
        "param_hr_core_dataset_name": bq_project_id + '.' + config['env']['v_hr_core_dataset_name'],
        "param_hr_base_views_dataset_name": bq_project_id + '.' + config['env']['v_hr_base_views_dataset_name'],
        "param_hr_views_dataset_name": bq_project_id + '.' + config['env']['v_hr_views_dataset_name'],
        "param_pub_views_dataset_name": bq_project_id + '.' + config['env']['v_pub_views_dataset_name'],
        }
    }

def create_dag( dag_id, schedule, tags, source_system, dependency):
    if schedule == "None":
        schedule = None
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args, 
        schedule_interval=schedule,
        catchup=False, 
        max_active_runs=1, 
        template_searchpath='/home/airflow/gcs/dags/sql/',
        tags=tags
    )
    with dag:

        #define tasks 
        start_job = DummyOperator(task_id='start_dag')
        end_job = DummyOperator(task_id='end_dag')

        for idx, item in enumerate(dependency, 1):
            has_sensor = item["source"].get("sensor")

            sourcesysname = source_system
            source_file_name_prefix = item["source"].get("source_file_name_prefix")
            source_file_extension = item["source"].get("source_file_extension")
            file_pattern = item["source"]["patterns"]
            file_name_sub = file_pattern[0]["pattern"].replace('^','')
            file_name_sub = file_name_sub.replace('_.*\.txt\.pgp$','')
            gsutil_command = f'gsutil -m cp gs://{src_bucket}/edwhrdata/srcfiles/enwisen/*{file_name_sub}* gs://"{tgt_bucket}/edwhrdata/srcfiles/enwisen/"'
            # file_pattern = item["source"].get("pattern")
            remove_date_marker = item["source"].get("remove_date_marker")
            if remove_date_marker == 'No':
                tgt_bq_table = item["target"].get("name")
                sql = item["target"].get("sql")
                is_prefixed_file_name = item["source"].get("is_prefixed_file_name")
                source_id = int(item["source"].get("source_id"))
                skip_leading_records = item["source"].get("skip_leading_records")
                delimiter = item["source"].get("delimiter")
                quote_char = item["source"].get("quote_char")
                encoding_scheme = item["source"].get("encoding_scheme")
                preprocessing_steps_string = item["source"].get("preprocessing_steps")
                preprocessing_steps = preprocessing_steps_string.split(',')
                private_key = config['env']['v_pwd_secrets_url'] + sftp_pgp_config['sftp_pgp_settings']['v_private_key']
                tolerance_percent = int(item["source"].get("tolerance_percent"))
            

            with TaskGroup(group_id=f'TG-table-{source_file_name_prefix}') as table_grp:

                sftp_get_files= PythonOperator(
                    task_id = f'sftp_get_files_{source_file_name_prefix}',
                    python_callable = cu.sftp_get_files_remove_date_marker,
                    provide_context = True,
                    op_kwargs = {
                        'sourcesysname' : source_system,
                        'remote_directory': directory,
                        'file_pattern': file_pattern,
                        'pwd_secret': pwd_secret, 
                        'host': host, 
                        'user': user,
                        'remove_date_marker': remove_date_marker,
                        'file_prefix': source_file_name_prefix,
                        'file_extension': source_file_extension
                    } 
                )
                if environment == "prod":
                        copy_data_task = BashOperator(
                                task_id=f'copy_data_to_{lower_environment}',
                                    bash_command=gsutil_command
                            )

                if env == 'prod':
                    delete_sftp_files= PythonOperator(
                        task_id = f'delete_sftp_files_{source_file_name_prefix}',
                        python_callable = cu.sftp_delete_files,
                        provide_context = True,
                        op_kwargs = {
                            'sourcesysname' : source_system,
                            'remote_directory': directory,
                            'file_pattern': file_pattern,
                            'pwd_secret': pwd_secret, 
                            'host': host, 
                            'user': user
                        } 
                    )

                if remove_date_marker == 'No':
                    preprocess_file = PythonOperator(
                        task_id=f'preprocess_file_{tgt_bq_table}',
                        provide_context=True,
                        python_callable=cu.preprocess_file_for_staging,
                        op_kwargs={
                            'sourcesysname': sourcesysname,
                            'source_file_name_prefix': source_file_name_prefix,
                            'source_file_extension': source_file_extension,
                            'bq_staging_dataset': bq_staging_dataset,
                            'tgt_bq_table': tgt_bq_table,
                            'delimiter': delimiter,
                            'quote_char': quote_char,
                            'is_prefixed_file_name': is_prefixed_file_name,
                            'encoding_scheme': encoding_scheme,
                            'preprocessing_steps': preprocessing_steps,
                            'private_key': private_key
                        }
                    )

                    load_gcsfile_tobq=GCSToBigQueryOperator(
                            task_id=f'load_gcsfile_tobq_{tgt_bq_table}',
                            bucket=gcs_bucket,
                            gcp_conn_id=config['env']['v_curated_conn_id'],
                            source_objects=['{}{}/{}.{}'.format(src_folder, sourcesysname, source_file_name_prefix,source_file_extension)],
                            destination_project_dataset_table='{}.{}.{}'.format(bq_project_id, bq_staging_dataset, tgt_bq_table),
                            write_disposition='WRITE_TRUNCATE',
                            field_delimiter=delimiter,
                            quote_character=quote_char,     
                            allow_quoted_newlines=True,
                            skip_leading_rows=skip_leading_records,
                            allow_jagged_rows = True,
                            max_bad_records=0,
                            schema_object='{}{}/{}.{}'.format(schema_folder, sourcesysname, tgt_bq_table, 'schema.json'),
                        )

                    update_bq_table=BigQueryOperator(
                        task_id=f'update_bq_table_{tgt_bq_table}',
                        gcp_conn_id=config['env']['v_curated_conn_id'],
                        use_legacy_sql=False,
                        retries=0,
                        query_params=[{ 'name': 'target', 'parameterType': { 'type': 'STRING' },'parameterValue': { 'value': tgt_bq_table } }],
                        sql=f'dml/ingest/{sql}'
                    )

                    audit_srcfile = PythonOperator(
                        task_id=f'audit_srcfile_{tgt_bq_table}',
                        provide_context=True,
                        python_callable=cu.insert_audit_data,
                        op_kwargs={
                            'sourcesysname': sourcesysname,
                            'source_file_name_prefix': source_file_name_prefix,
                            'source_file_extension': source_file_extension,
                            'tgt_bq_table': tgt_bq_table,
                            'encoding_scheme': encoding_scheme,
                            'source_id': source_id,
                            'bq_staging_dataset': bq_staging_dataset,
                            'dag_id': dag_id,
                            'load_start_time': load_start_time,
                            'tolerance_percent': tolerance_percent
                        }
                    )

                    archive_srcfile = PythonOperator(
                        task_id=f'archive_srcfile_{tgt_bq_table}',
                        provide_context=True,
                        python_callable=cu.archive_file_and_cleanup,
                        op_kwargs={
                            'sourcesysname': sourcesysname,
                            'source_file_name_prefix': source_file_name_prefix,
                            'source_file_extension': source_file_extension,
                            'tgt_bq_table': tgt_bq_table,
                            'encoding_scheme': encoding_scheme
                        }
                    )

                    if has_sensor == 'Yes':
                        for sensor in item["sensor"]:
                            ext_dag_id = sensor.get("dag_id")
                            ext_task_id = sensor.get("task_id")
                            schedule = sensor.get("schedule")
                            with TaskGroup(group_id=f'TG-sensor-{tgt_bq_table}') as sensor_grp:
                                check_task_completion= ExternalTaskSensor(
                                    task_id=f"Check_status_{ext_task_id}",
                                    external_dag_id=ext_dag_id,
                                    external_task_id=ext_task_id,
                                    timeout=600,
                                    execution_date_fn=cu.get_execution_date,
                                    params={"schedule":schedule},
                                    allowed_states=["success"],
                                    failed_states=["failed", "skipped"],
                                    mode="reschedule"
                                )
                                [check_task_completion]
                        if env == 'prod':
                            [sensor_grp] >> sftp_get_files >> copy_data_task >> preprocess_file >> load_gcsfile_tobq >> update_bq_table >> audit_srcfile >> archive_srcfile >> delete_sftp_files
                        else:
                            [sensor_grp] >> sftp_get_files >> preprocess_file >> load_gcsfile_tobq >> update_bq_table >> audit_srcfile >> archive_srcfile
                    else:
                        if env == 'prod':
                            sftp_get_files >> copy_data_task >> preprocess_file >> load_gcsfile_tobq >> update_bq_table >> audit_srcfile >> archive_srcfile >> delete_sftp_files
                        else:
                            sftp_get_files >> preprocess_file >> load_gcsfile_tobq >> update_bq_table >> audit_srcfile >> archive_srcfile
                else:
                    if env == 'prod':
                        sftp_get_files >> copy_data_task >> delete_sftp_files
                    else:
                        sftp_get_files
                
        #setting dag dependency

            start_job >> [table_grp] >> end_job
        return dag

config_enwisen = cu.call_config_yaml("hrg_enwisen_ingest_dependency.yaml","hca_hrg_enwisen_ingest_dependency")
# dependencies = config_enwisen
for ingest in config_enwisen['ingest']:
    frequency = ingest["frequency"]
    schedule = ingest["schedule"]
    source_system = ingest["source_system"]
    type = ingest["type"]

    if schedule == "None":
        interval_range =  "None"
    else:
        time = ingest["schedule"].split(" ")
        interval_range = time[1].zfill(2) + "."  + time[0].zfill(2)
    
    dependency = ingest["dependency"]
    tags = [source_system]
    if frequency == "None":
        dag_id = f'dag_ingest_' + source_system + '_' + type + '_adhoc'
        tags.extend(['adhoc'])
    else:
        dag_id = f'dag_ingest_' + source_system + '_' + type + '_' + frequency + '_' + interval_range
        tags.extend([frequency])
    additional_tags = ingest["custom_tags"]
    tags.extend(additional_tags.split(','))
    globals()[dag_id] = create_dag(dag_id, schedule, tags, source_system, dependency)