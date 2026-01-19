from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GCSToBigQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta
import pandas as pd
import time
import pendulum
import os
import sys
import logging
import json

script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, '..', 'utils')
sys.path.append(util_dir)
# import common_utilities after setting the path for utils
import common_utilities as cu

current_timezone = pendulum.timezone("US/Central")

config = cu.call_config_yaml("hrg_config.yaml","hca_hrg_default_vars")
# get project variables from config file
bq_project_id = config['env']['v_curated_project_id']
bq_staging_dataset = config['env']['v_hr_stage_dataset_name']
gcs_bucket = config['env']['v_data_bucket_name']
src_folder = config['env']['v_srcfilesdir']
arc_folder = config['env']['v_archivedir']
tmp_folder = config['env']['v_tmpobjdir']
schema_folder = tmp_folder + config['env']['v_schemasubdir']

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
        "param_source_system_code": 'T',
        }
    }

def create_dag(dag_id, schedule, tags, source_system,filelist,Preprocessing,done_file):
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

        num_files = len(filelist)
        logging.info("===Number of files {}===".format(str(num_files)))
        # logging.info("===Begin {}".format(time.strftime("%Y%m%d-%H:%M:%S")))
        sourcesysname = source_system
        try:
            if frequency== "daily":
                now = pendulum.now(current_timezone).strftime("%Y%m%d")
                prev = (pendulum.now(current_timezone).subtract(days=7)).strftime("%Y%m%d")
            elif frequency== "weekly":
                now = pendulum.yesterday(current_timezone).strftime("%Y%m%d")
                prev = (pendulum.yesterday(current_timezone).subtract(days=7)).strftime("%Y%m%d")
        except:
            logging.info(
                "================={} is not a valid frequency(daily/weekly)=================".format(frequency))
        
        done_file_to_delete =  done_file.replace('YYYYMMDD', prev)     
        done_file= done_file.replace('YYYYMMDD', now)  
        file_sensor = GCSObjectsWithPrefixExistenceSensor(
            bucket=gcs_bucket,
            prefix=src_folder + source_system + "/" + done_file,
            timeout=10800,
            mode="reschedule",
            task_id="check_taleo_done_file_exists"
        )
        delete_old_done_files = PythonOperator(
                task_id='delete_old_done_files',
                python_callable=cu.removegcsfileifexists,
                op_kwargs={
                    'sourcesysname' : sourcesysname,
                    'folder' : src_folder,
                    'filename' : done_file_to_delete
                }
                )

        for fileinfo in filelist: 
            srcfileid = fileinfo.split("|")[0]
            srcfilename = fileinfo.split("|")[1].lower()
            source_file_name_prefix=srcfilename.split(".")[0]
            source_file_extension=srcfilename.split(".")[1]
            tgttablename = fileinfo.split("|")[2].lower()
            Preprocessing_list=fileinfo.split("|")[3]
            sql=fileinfo.split("|")[4].lower()

            delimiter = Preprocessing['delimiter']
            encoding_scheme=Preprocessing['encoding_scheme']
            is_prefixed_file_name=Preprocessing['is_prefixed_file_name']
            quote_char = Preprocessing['quote_char']
            skip_leading_records = Preprocessing['skip_leading_records']
            tableload_start_time = str(pendulum.now(current_timezone))[:23]

            with TaskGroup(group_id=f'TG-table-{tgttablename}') as table_grp:


                with TaskGroup(group_id=f'TG-Preprocessing-{tgttablename}') as preprocessing_grp:

                    expected_count = PythonOperator(
                        task_id='get_expected_count_for_' + tgttablename,
                        python_callable=cu.expected_count_from_src,
                        op_kwargs={
                            'src_folder':src_folder,
                            'sourcesysname': sourcesysname,
                            'source_file_name_prefix': source_file_name_prefix,
                            'source_file_extension': source_file_extension,
                            'encoding_scheme': encoding_scheme,
                        }     
                    ) 

                    if Preprocessing_list!='None':
                        preprocessing_steps_string=Preprocessing[Preprocessing_list]
                        preprocessing_steps = preprocessing_steps_string.split(',')                    
                        preprocess_file = PythonOperator(
                        task_id=f'preprocess_file_{srcfilename}',
                        provide_context=True,
                        python_callable=cu.preprocess_file_for_staging,
                        op_kwargs={
                        'sourcesysname': sourcesysname,
                        'source_file_name_prefix': source_file_name_prefix,
                        'source_file_extension': source_file_extension,
                        'bq_staging_dataset': bq_staging_dataset,
                        'tgt_bq_table': tgttablename,
                        'delimiter': delimiter,
                        'quote_char': quote_char,
                        'is_prefixed_file_name': is_prefixed_file_name,
                        'encoding_scheme': encoding_scheme,
                        'preprocessing_steps': preprocessing_steps
                        }
                        )
                        expected_count >> [preprocess_file]
                    else:                   
                        get_table_ddl = PythonOperator(
                        task_id=f'Schema_for_{srcfilename}',
                        provide_context=True,
                        python_callable=cu.getschema,
                        op_kwargs={
                        'sourcesysname': sourcesysname,
                        'tgttablename': tgttablename
                        }
                        )
                        expected_count >> [get_table_ddl]


                load_gcsfile_tobq=GCSToBigQueryOperator(
                        task_id=f'load_gcsfile_tobq_{tgttablename}',
                        bucket=config['env']['v_data_bucket_name'],
                        gcp_conn_id=config['env']['v_curated_conn_id'],
                        source_objects=['{}{}/{}'.format(src_folder, sourcesysname, srcfilename)],
                        destination_project_dataset_table=bq_staging_dataset+ '.' + tgttablename,
                        write_disposition='WRITE_TRUNCATE',
                        field_delimiter=delimiter,
                        quote_character=quote_char,     
                        allow_quoted_newlines=True,
                        skip_leading_rows=skip_leading_records,
                        allow_jagged_rows = True,
                        max_bad_records=0,
                        schema_object='{}{}/{}.{}'.format(schema_folder, sourcesysname, tgttablename, 'schema.json'),
                    )

                update_bq_table = BigQueryOperator(
                    task_id=f'update_bq_table_{tgttablename}',
                    gcp_conn_id=config['env']['v_curated_conn_id'],
                    query_params=[{ 'name': 'target', 'parameterType': { 'type': 'STRING' },'parameterValue': { 'value': tgttablename } }],
                    sql = f'dml/ingest/{sql}',
                    use_legacy_sql=False,
                    retries=0,
                    )


                archive_srcfile = PythonOperator(
                    task_id=f'archive_srcfile_{tgttablename}',
                    provide_context=True,
                    python_callable=cu.archive_file_and_cleanup,
                    op_kwargs={
                    'sourcesysname': sourcesysname,
                    'source_file_name_prefix': source_file_name_prefix,
                    'source_file_extension': source_file_extension,
                    'tgt_bq_table': tgttablename,
                    'encoding_scheme': encoding_scheme
                    }
                    )      

                target_count = PythonOperator(
                    task_id='get_actual_count_for_' + tgttablename,
                    python_callable=cu.get_count,
                    op_kwargs={
                    'bq_staging_dataset': bq_staging_dataset,
                    'tgttablename': tgttablename,
                    }       
                )               

                audit_check = PythonOperator(
                    task_id='adding_audit_entry_for_' + tgttablename,
                    python_callable=cu.audit_table,
                    op_kwargs={
                        'dag_id': dag_id,
                        'srcfileid': srcfileid,
                        'srcfilename': srcfilename,
                        'sourcesysname': sourcesysname,
                        'tgttablename': tgttablename,
                        'src_rec_count': f"{{{{ task_instance.xcom_pull(task_ids= 'TG-table-{tgttablename}.TG-Preprocessing-{tgttablename}.get_expected_count_for_{tgttablename}') }}}}",
                        'tgt_rec_count': f"{{{{ task_instance.xcom_pull(task_ids= 'TG-table-{tgttablename}.get_actual_count_for_{tgttablename}') }}}}",
                        'tableload_start_time': tableload_start_time,
                    }       
                )


                [preprocessing_grp] >> load_gcsfile_tobq >> update_bq_table >> archive_srcfile >> target_count >> audit_check

            if done_file == "None":
        #setting dag dependency
                start_job >> [table_grp] >> end_job
            else:
                start_job >> file_sensor >> [table_grp] >> delete_old_done_files >> end_job
    return dag

config_taleo = cu.call_config_yaml("hrg_taleo_ingest_dependency.yaml","hca_hrg_taleo_ingest_dependency")
# dependencies = config_taleo
for ingest in config_taleo['ingest']:
    frequency = ingest["frequency"]
    schedule = ingest["schedule"]
    source_system = ingest["source_system"]
    type = ingest["type"]
    done_file = ingest["done_file"]
    filelist=ingest['srctablelist']
    Preprocessing=ingest['Preprocessing']
    if schedule == "None":
        interval_range =  "None"
    else:
        time = ingest["schedule"].split(" ")
        interval_range = time[1].zfill(2) + "."  + time[0].zfill(2)
    tags = [source_system]
    if frequency == "None":
        dag_id = f'dag_ingest_' + source_system + '_' + type + '_adhoc'
        tags.extend(['adhoc'])
    else:
        dag_id = f'dag_ingest_' + source_system + '_' + type + '_' + frequency + '_' + interval_range
        tags.extend([frequency])
    additional_tags = ingest["custom_tags"]
    tags.extend(additional_tags.split(','))
    globals()[dag_id] = create_dag(dag_id, schedule, tags, source_system,filelist,Preprocessing,done_file)