from airflow import DAG
from airflow.models import Variable

from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GCSToBigQueryOperator
from airflow.contrib.operators.gcs_to_gcs import GCSToGCSOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import  DummyOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator


import re
import json 
import yaml
from datetime import datetime, timedelta
import os
import sys
import pendulum
import pandas as pd
import logging
import openpyxl
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateExternalTableOperator,
    BigQueryDeleteDatasetOperator,
)

script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, '..', 'utils')
sys.path.append(util_dir)
import common_utilities as cu

config = cu.call_config_yaml("hrg_config.yaml","hca_hrg_default_vars")

bq_project_id = config['env']['v_curated_project_id']
bq_landing_project_id = config['env']['v_landing_project_id']
bq_staging_dataset = config['env']['v_hr_stage_dataset_name']
stage_dataset = config['env']['v_hr_stage_dataset_name']
gcs_bucket = config['env']['v_data_bucket_name']
glint_gcs_bucket = config['env']['v_glint_data_bucket_name']
src_folder = config['env']['v_srcfilesdir']
arc_folder = config['env']['v_archivedir']
tgt_folder = config['env']['v_tgtfilesdir']
tmp_folder = config['env']['v_tmpobjdir']
schema_folder = tmp_folder + config['env']['v_schemasubdir']
timezone = pendulum.timezone("US/Central")

def get_execution_date(execution_date, **kwargs):  
# Function used in execution_date_fn of ExternalTaskSensor task
    print("==== In get_exceution_date with params ====")
    print(execution_date, kwargs)
    exec_day = int(execution_date.strftime('%w'))
    print(exec_day)
    schedule = kwargs["params"]["schedule"]
    print(schedule)
    for item in schedule:
        if len(schedule) == 1:
            schedule = item
        else:
            now = pendulum.now(timezone)
            if now.month <= int(item.split(" ")[3]):
                schedule = item
                break
    print(schedule)
    schedule_day = schedule.split()[4]
    if schedule_day == "*":
        poke_date = execution_date
    elif exec_day <= int(schedule_day):
        date_diff = 7-int(schedule_day)+exec_day
        print(date_diff)
        print(execution_date - timedelta(days=date_diff))
        poke_date = execution_date - timedelta(days=date_diff)
        
    elif exec_day > int(schedule_day):
        date_diff = exec_day-int(schedule_day)
        print(execution_date - timedelta(days=date_diff))
        poke_date = execution_date - timedelta(days=date_diff)

    year = int(poke_date.year)
    month = int(poke_date.month)
    date = int(poke_date.day)
    min = int(schedule.split()[0])
    hour = int(schedule.split()[1])
    poke_date = pendulum.datetime(year,month,date,hour,min,0,tz=timezone)
    print(poke_date)
    return poke_date

def get_blob(bucket, folder, sysname, objectname):
    # Get full path of the file
    try:
        objectfullpath = '{}{}/{}'.format(folder, sysname, objectname)
        blob = bucket.blob(objectfullpath)
        logging.info(f"found object {objectfullpath}")
        return blob
    except:
        logging.info("Blob is not found or instantiated {} in folder {} and system directory {}".format(
            objectname, folder, sysname))
        raise SystemExit()
    
def excel_to_csv(src_folder_path,srcfilename,sourcesysname,delimiter,header,quotechar,quoting,escape_char,encoding,rename):
    logging.info("==== In filetodf with params:=====")
    logging.info(src_folder_path, srcfilename, delimiter, header,
                 quotechar, quoting, escape_char, encoding)
    bucket_name = gcs_bucket
    blob_name = src_folder_path + sourcesysname + "/" + srcfilename
    print(blob_name)

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    data_bytes = blob.download_as_bytes()

    df = pd.read_excel(data_bytes, index_col=None, engine="openpyxl")
    print(df)

    blob = get_blob(bucket, src_folder_path, sourcesysname,rename)
    blob.upload_from_string(df.to_csv(sep=delimiter, quotechar=quotechar,
                            encoding=encoding, quoting=quoting, escapechar=escape_char, index=False))
    logging.info("File {} loaded to bucket: {}".format(
        rename, src_folder_path+sourcesysname))

    

default_args = {
    'owner': 'hca_hrg_atos',
    'depends_on_past': False,
    'email': config['env']['v_failure_email_list'],
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=240),
    #params to substitute in sql files
    'params': {
        "param_hr_stage_dataset_name": config['env']['v_curated_project_id'] + 
                                    '.' +  config['env']['v_hr_stage_dataset_name'],
        "param_hr_core_dataset_name": config['env']['v_curated_project_id'] + 
                                    '.' + config['env']['v_hr_core_dataset_name'],
        }
    }

def create_dag( dag_id, schedule, start_date, dependency,source_system,frequency,check_file_prefix, integrate_dag):
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args, 
        start_date = eval(start_date),
        schedule_interval=schedule,
        catchup=False, 
        max_active_runs=1, 
        template_searchpath='/home/airflow/gcs/dags/sql/',
        tags=[f"{source_system}", f"{frequency}"]
        
    )
    with dag:

        #define tasks 
        start_job = DummyOperator(task_id='start_dag')
        end_job = DummyOperator(task_id='end_dag')

        check_file_sensor = GCSObjectsWithPrefixExistenceSensor(
                    task_id='check_gcs_file',
                    bucket=config['env']['v_glint_data_bucket_name'],
                    prefix=check_file_prefix,
                    timeout=30,
                    soft_fail=True
                )
        
        trigger_operator=TriggerDagRunOperator(
                    task_id='trigger_integrate_dag',
                    trigger_dag_id=integrate_dag
                )
        
        archive_xls_srcfile=GCSToGCSOperator(
                    task_id=f'archive_xls_srcfile',
                    source_bucket=config['env']['v_data_bucket_name'],
                    destination_bucket=config['env']['v_data_archive_bucket_name'],
                    source_object=config['env']['v_srcfilesdir'] +source_system + "/",
                    destination_object=config['env']['v_archivedir'] + source_system +'/',
                    move_object=True        
                )
        
        table_grp_arr = []
        for ind, item in enumerate(dependency):
            source = item["source"].get("name")
            target = item["target"].get("name")
            rename = item["source"].get("rename")
            pattern = item["source"].get("search_pattern")
            sql = item["target"].get("sql")
            has_sensor = item["source"].get("sensor")
            encoding_scheme = item["source"].get("encoding_scheme")
            skip_leading_records = item["source"].get("skip_leading_records")
            tolerance_percent = item["source"].get("tolerance_percent")
            source_id = item["source"].get("source_id")
            load_start_time = pendulum.now('US/Central').to_datetime_string()
            bqdatasetfullname = config['env']['v_curated_project_id'] + ':' + config['env']['v_hr_stage_dataset_name'] +  '.' + target
            bqschemaobjname = config['env']['v_hr_stage_dataset_name'] +  '.' + target + ".json"
            gcsbqschemaobjtempfolder = "gs://"  + config['env']['v_data_bucket_name']  + "/edwhrdata/tmp"
            

              #Assign default values if not configured in YAML
            if item["source"].get("delimiter"):
                delimiter = item["source"].get("delimiter")
            else:
                delimiter = '|'
            if item["source"].get("header"):
                header = item["source"].get("header")
            else:
                header = 0
            if item["source"].get("quotechar"):
                quotechar = item["source"].get("quotechar")
            else:
                quotechar = "\""
            if item["source"].get("quoting"):
                quoting = item["source"].get("quoting")
            else:
                quoting = 1
            if item["source"].get("escape_char"):
                escape_char = item["source"].get("escape_char")
            else:
                escape_char = "\\"
            if item["source"].get("encoding"):
                encoding = item["source"].get("encoding")
            else:
                encoding = "utf-8"
            
            

            with TaskGroup(group_id=f'TG-table-{target}') as table_grp:

                move_files_from_source_bucket_landing = GCSToGCSOperator(
                    task_id=f'move_files_from_source_bucket_{target}',
                    source_bucket=config['env']['v_glint_data_bucket_name'],
                    destination_bucket=config['env']['v_data_bucket_name'],
                    source_object=source,
                    destination_object=config['env']['v_srcfilesdir'] +source_system+'/'+source.split("*")[0],
                    move_object=True        
                )

                merge_files_matching_prefix = PythonOperator(
                        task_id = f'merge_files_matching_prefix_{target}',
                        python_callable = cu.listblobswithpattern,
                        op_kwargs = {
                            'src_folder_path': 'edwhrdata/srcfiles/',
                            'srcfilename' : pattern,
                            'delimiter': delimiter,
                            'header' : header,
                            'quotechar' : quotechar,
                            'quoting' : quoting,
                            'sourcesysname' : source_system,
                            'escape_char' : escape_char,
                            'encoding':encoding,
                            'merge': item["source"].get("merge")
                        } 
                )

                convert_excel_to_csv = PythonOperator(
                        task_id = f'convert_excel_to_csv_{target}',
                        python_callable = excel_to_csv,
                        op_kwargs = {
                            'src_folder_path': 'edwhrdata/srcfiles/',
                            'function': item["source"].get("pre_process"),
                            'srcfilename': f"{{{{ task_instance.xcom_pull(task_ids= 'TG-table-{target}.merge_files_matching_prefix_{target}') }}}}",
                            'sourcesysname' : source_system,
                            'delimiter': delimiter,
                            'header': header,
                            'quotechar': quotechar,
                            'quoting': quoting,                                                    
                            'escape_char': escape_char,
                            'encoding': encoding,
                            'rename': rename
                        }                
                )
                      
                update_bq_table = BigQueryOperator(
                    task_id=f'update_bq_table_{target}',
                    gcp_conn_id=config['env']['v_curated_conn_id'],
                    query_params=[{ 'name': 'target', 'parameterType': { 'type': 'STRING' },'parameterValue': { 'value': target } }],
                    sql = f'dml/ingest/{sql}',
                    use_legacy_sql=False, 
                    retries=0, 
                )

                audit_srcfile = PythonOperator(
                        task_id=f'audit_srcfile_{target}',
                        provide_context=True,
                        python_callable=cu.insert_audit_data_files,
                        op_kwargs={
                            'config': config,
                            'sourcesysname': source_system,
                            'source_file_name' : rename,
                            'tgt_bq_table': target,
                            'encoding_scheme': encoding_scheme,
                            'source_id': source_id,
                            'bq_project_id': bq_project_id,
                            'gcs_bucket': gcs_bucket,
                            'src_folder': src_folder,
                            'bq_staging_dataset': bq_staging_dataset,
                            'dag_id': dag_id,
                            'load_start_time': load_start_time,
                            'tolerance_percent': tolerance_percent
                        }
                )

                file=rename.split(".")
                archive_srcfile=GCSToGCSOperator(
                    task_id=f'archive_srcfile_{target}',
                    source_bucket=config['env']['v_data_bucket_name'],
                    destination_bucket=config['env']['v_data_archive_bucket_name'],
                    source_object=config['env']['v_srcfilesdir'] +source_system + "/" + rename,
                    destination_object=config['env']['v_archivedir'] + source_system + "/" + file[0] + datetime.now().strftime("%m-%d-%Y") +".csv",
                    move_object=True        
                )

                                 
                if has_sensor == 'Yes':
                    for sensor in item["sensor"]:
                        ext_dag_id = sensor.get("dag_id")
                        ext_task_id = sensor.get("task_id")
                        schedule = sensor.get("schedule")
                        with TaskGroup(group_id=f'TG-sensor-{target}') as sensor_grp:
                            check_task_completion= ExternalTaskSensor(
                                task_id=f"Check_status_{ext_task_id}",
                                external_dag_id=ext_dag_id,
                                external_task_id=ext_task_id,
                                timeout=600,
                                execution_date_fn=get_execution_date,
                                params={"schedule":schedule},
                                allowed_states=["success"],
                                failed_states=["failed", "skipped"],
                                mode="reschedule"
                            )
                            [check_task_completion]
                    [sensor_grp] >> move_files_from_source_bucket_landing >> merge_files_matching_prefix >> convert_excel_to_csv >>  update_bq_table >>  audit_srcfile >> archive_srcfile 
                elif rename == 'HCA_Engagement_Vertical.csv':
                        
                        create_external_table = BigQueryCreateExternalTableOperator(
                            task_id="create_external_table",
                            destination_project_dataset_table=config['env']['v_curated_project_id'] + 
                                            '.' +  config['env']['v_hr_stage_dataset_name'] + '.' + "glint_engagement_external_table",
                            bucket=config['env']['v_data_bucket_name'],
                            source_objects=['edwhrdata/srcfiles/'+ source_system + '/' + rename],
                            source_format='CSV',
                            autodetect=True,
                            skip_leading_rows=1
                        )
                        move_files_from_source_bucket_landing >> merge_files_matching_prefix >> convert_excel_to_csv >> create_external_table >> update_bq_table >> audit_srcfile >> archive_srcfile
                else:
                    get_table_ddl = BashOperator(
                        task_id=f'get_table_ddl_{target}',
                        #run bq to get json formatted schema_object 
                        #run gsutil copy  schema_object  to GCS /dags/utils/temp/ folder
                        #remove after gsutil cp
                        bash_command="bq show --schema --format=json  " + bqdatasetfullname + " > "  + bqschemaobjname
                                                + " && gsutil cp "  + bqschemaobjname + " " + gcsbqschemaobjtempfolder
                                                + " && rm " + bqschemaobjname
                    )
            
                    load_gcsfile_tobq=GCSToBigQueryOperator(
                        task_id=f'load_gcsfile_tobq_{target}',
                        bucket=config['env']['v_data_bucket_name'],
                        gcp_conn_id=config['env']['v_curated_conn_id'],
                        source_objects=config['env']['v_srcfilesdir']  + source_system + "/" + rename,
                        destination_project_dataset_table=config['env']['v_curated_project_id'] + 
                                                    '.' + config['env']['v_hr_stage_dataset_name'] + 
                                                    '.' + target,
                        write_disposition='WRITE_TRUNCATE',
                        field_delimiter=delimiter,
                        quote_character=quotechar,     
                        skip_leading_rows=header,    
                        allow_quoted_newlines=True,
                        allow_jagged_rows=True, 
                        #using schema object generated and saved in GCS
                        schema_object='edwhrdata/tmp/' + bqschemaobjname
                    )
                    move_files_from_source_bucket_landing >> merge_files_matching_prefix >> convert_excel_to_csv >>  get_table_ddl >> load_gcsfile_tobq  >> update_bq_table >> audit_srcfile >> archive_srcfile

            if ind == 0:
                start_job >> check_file_sensor >> table_grp
            else:
                table_grp_arr[-1] >> table_grp >> archive_xls_srcfile >> trigger_operator >> end_job
            table_grp_arr.append(table_grp)

        

        return dag

config_glint = cu.call_config_yaml("hrg_glint_ingest_dependency.yaml","hca_hrg_glint_ingest_dependency")
for ingest in config_glint['ingest']:
    frequency = ingest["frequency"]
    schedule = ingest["schedule"]
    start_date = ingest["start_date"]
    source_system = ingest["source_system"]
    type = ingest["type"]
    dependency = ingest["dependency"]
    check_file_prefix = ingest["check_file_prefix"]    
    integrate_dag = ingest["integrate_dag"]

    for item in schedule:
        if len(schedule) == 1:
            if item == "None":
                interval_range =  "None"
                schedule = None
            else:
                schedule = item
                time = item.split(" ")
                interval_range = time[1].zfill(2) + "."  + time[0].zfill(2)
        else:
            now = pendulum.now(timezone)
            if now.month <= int(item.split(" ")[3]):
                schedule = item
                time = item.split(" ")
                interval_range = time[1].zfill(2) + "."  + time[0].zfill(2)
                break

    dag_id = f'dag_ingest_'+source_system+'_'+type+'_' + frequency + '_' + interval_range
    globals()[dag_id] = create_dag(dag_id, schedule, start_date, dependency,source_system,frequency,check_file_prefix,integrate_dag)