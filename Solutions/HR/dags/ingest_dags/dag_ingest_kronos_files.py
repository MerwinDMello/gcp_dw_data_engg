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
from google.cloud import storage


import json 
import yaml
from datetime import datetime, timedelta
import os
import sys
import pendulum
import pandas as pd
import uuid
from io import StringIO
import csv

script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, '..', 'utils')
sys.path.append(util_dir)
import common_utilities as cu

config = cu.call_config_yaml("hrg_config.yaml","hca_hrg_default_vars")
src_folder = config['env']['v_srcfilesdir']

bq_src_project_id = config['env']['v_landing_project_id']
src_bucket = config['env']['v_data_bucket_name']
bq_tgt_project_id = config['low_env']['v_landing_project_id']
tgt_bucket = config['low_env']['v_data_bucket_name']

environment = config['env']['v_env_name']
lower_environment = config['low_env']['v_env_name']
gsutil_command = f'gsutil -m cp gs://{src_bucket}/edwhrdata/srcfiles/kronos/*.txt gs://"{tgt_bucket}/edwhrdata/srcfiles/kronos/"'


timezone = pendulum.timezone("US/Central")
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

def get_execution_date(execution_date, **kwargs):  
    print(execution_date)  
    exec_day = int(execution_date.strftime('%w'))
    print(exec_day)
    schedule = kwargs["params"]["schedule"]
    print(schedule)
    schedule_day = schedule.split()[4]
    if schedule_day == "*":
        poke_date = execution_date
    elif exec_day <= int(schedule_day):
        date_diff = 7-int(schedule_day)+exec_day
        print(date_diff)
        print(execution_date - timedelta(days=date_diff))
        poke_date = execution_date - timedelta(days=date_diff)
        poke_date = poke_date - timedelta(days=7)
        
    elif exec_day > int(schedule_day):
        date_diff = exec_day-int(schedule_day)
        print(execution_date - timedelta(days=date_diff))
        poke_date = execution_date - timedelta(days=date_diff)
        poke_date = poke_date - timedelta(days=7)
    
    print(poke_date.year)
    year = int(poke_date.year)
    month = int(poke_date.month)
    date = int(poke_date.day)
    min = int(schedule.split()[0])
    hour = int(schedule.split()[1])
    poke_date = pendulum.datetime(year,month,date,hour,min,0,tz=timezone)
    print(poke_date)
    return poke_date 
   
def insert_audit_table_entry(**kwargs):                  
    bqproject_id = config['env']['v_curated_project_id']

    tgt_count_query = "select count(1) as TGT_COUNT from " + \
        config['env']['v_hr_stage_dataset_name'] + '.' + kwargs['tgttablename']
    tgt_rec_count_df = pd.read_gbq(
        tgt_count_query, project_id=bqproject_id)
    tgt_rec_count = tgt_rec_count_df['TGT_COUNT'][0]

    tableload_start_time = str(pendulum.parse(kwargs['tableload_start_time']).in_timezone(timezone))[:26]
    tableload_end_time = str(pendulum.now(timezone).in_timezone(timezone))[:26]
    tableload_run_time = (pd.to_datetime(tableload_end_time) - pd.to_datetime(tableload_start_time))
    
    data_bucket = cu.get_bucket(config['env']['v_landing_project_id'], config['env']['v_data_bucket_name'])
    done_file_blob = cu.get_blob(data_bucket, config['env']['v_srcfilesdir'], kwargs['sourcesysnm'], kwargs['done_file'])
    done_file_content = done_file_blob.download_as_string()
    done_file_content  = done_file_content.decode('utf-8')
    done_file_content = StringIO(done_file_content) 

    file_rec_count = csv.reader(done_file_content, delimiter=' ')
    for item in file_rec_count:
        if kwargs['srcfilename'] in item[1]:
            src_rec_count = int(item[0])
    
    if src_rec_count == tgt_rec_count:
        audit_status = 'PASS'
    elif tgt_rec_count > src_rec_count:
        audit_status = 'PASS(More records in Target)'
    else:
        audit_status = 'FAIL'
    
    audit_update_query = f"""INSERT INTO {config['env']['v_audittablename']} VALUES(
                                    '{str(uuid.uuid4())}', 
                                    {kwargs['srcfileid']}, 
                                    '{kwargs['sourcesysnm']}',
                                    '{kwargs['srcfilename']}',
                                    '{config['env']['v_hr_stage_dataset_name']}.{kwargs['tgttablename']}',
                                    'RECORD_COUNT', 
                                    {src_rec_count},
                                    {tgt_rec_count},
                                    '{tableload_start_time}',
                                    '{tableload_end_time}', 
                                    '{tableload_run_time}',
                                    '{kwargs['ti'].dag_id}-{pendulum.now(timezone).strftime("%Y%m%d%H%M%S")}',
                                    '{str(pendulum.now(timezone))[:23]}',
                                    '{audit_status}')"""

    audit_entry = pd.read_gbq(
        audit_update_query,  project_id=bqproject_id, max_results=0)
    

    
def create_dag( dag_id, schedule, start_date, dependency,source_system,frequency,done_file):
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
       
        now = pendulum.now(timezone).strftime("%Y%m%d")
        prev = (pendulum.now(timezone).subtract(days=7)).strftime("%Y%m%d")
        done_file_to_delete =  done_file.replace('YYYYMMDD', prev)        
        done_file= done_file.replace('YYYYMMDD', now) 
        if done_file != "None":
            file_sensor = GCSObjectsWithPrefixExistenceSensor(
                bucket=config['env']['v_data_bucket_name'],
                prefix=config['env']['v_srcfilesdir']  + source_system + "/" + done_file,
                timeout=10800,
                mode="reschedule",
                task_id="check_kronos_done_file_exists"
            )
            
            delete_old_done_files = PythonOperator(
                    task_id='delete_old_done_files',
                    python_callable=cu.removegcsfileifexists,
                    op_kwargs={
                        'sourcesysname' : source_system,
                        'folder' : src_folder,
                        'filename' : done_file_to_delete
                    }
                    )
            
        if environment == "prod":
            copy_data_task = BashOperator(
                    task_id=f'copy_data_to_{lower_environment}',
                        bash_command=gsutil_command
                )

            
    
                
            

        for item_id, item in enumerate(dependency):
            source = item["source"].get("name")
            target = item["target"].get("name")
            sql = item["target"].get("sql")
            has_sensor = item["source"].get("sensor")
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

                merge_files_matching_prefix = PythonOperator(
                        task_id = f'merge_files_matching_prefix_{target}',
                        python_callable = cu.listblobswithpattern,
                        op_kwargs = {
                            'src_folder_path': 'edwhrdata/srcfiles/',
                            'srcfilename' : source,
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

                preprocessfile = PythonOperator(
                        task_id = f'pre_process_{target}',
                        python_callable = cu.pre_process,
                        op_kwargs = {
                            'src_folder_path': 'edwhrdata/srcfiles/'+source_system,
                            'function': item["source"].get("pre_process"),
                            'srcfilename': f"{{{{ task_instance.xcom_pull(task_ids= 'TG-table-{target}.merge_files_matching_prefix_{target}') }}}}",
                            'sourcesysname' : source_system,
                            'delimiter': delimiter,
                            'header': header,
                            'quotechar': quotechar,
                            'quoting': quoting,                                                    
                            'escape_char': escape_char,
                            'encoding': encoding
                        }                
                )
                
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
                    source_objects=config['env']['v_srcfilesdir']  + source_system + "/" f"{{{{ task_instance.xcom_pull(task_ids= 'TG-table-{target}.merge_files_matching_prefix_{target}') }}}}",
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

                update_bq_table = BigQueryOperator(
                    task_id=f'update_bq_table_{target}',
                    gcp_conn_id=config['env']['v_curated_conn_id'],
                    query_params=[{ 'name': 'target', 'parameterType': { 'type': 'STRING' },'parameterValue': { 'value': target } }],
                    sql = f'dml/ingest/{sql}',
                    use_legacy_sql=False, 
                    retries=0, 
                )


                file=source.split(".")
                archive_srcfile=GCSToGCSOperator(
                    task_id=f'archive_srcfile_{target}',
                    source_bucket=config['env']['v_data_bucket_name'],
                    destination_bucket=config['env']['v_data_archive_bucket_name'],
                    source_object=config['env']['v_srcfilesdir']  + source_system + "/" + f"{{{{ task_instance.xcom_pull(task_ids= 'TG-table-{target}.merge_files_matching_prefix_{target}') }}}}",
                    destination_object=config['env']['v_archivedir'] + source_system + "/" + file[0] + datetime.now().strftime("%m-%d-%Y") +"."+ file[1],
                    move_object=True        
                )

                insert_audit_entry = PythonOperator(
                        task_id = f'insert_audit_entry_{target}',
                        python_callable = insert_audit_table_entry,
                        op_kwargs = {
                            'srcfilename' : source,
                            'sourcesysnm' : source_system,
                            'tgttablename': target,
                            'srcfileid': item_id+1,
                            'done_file' : done_file,
                            'tableload_start_time': f"{{{{ dag_run.get_task_instance('TG-table-{target}.load_gcsfile_tobq_{target}').start_date }}}}"
                        }                
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
                    [sensor_grp] >> merge_files_matching_prefix >> preprocessfile >> get_table_ddl  >>  load_gcsfile_tobq >> update_bq_table >> archive_srcfile >> insert_audit_entry
                else: 
                    merge_files_matching_prefix >> preprocessfile >> get_table_ddl  >>  load_gcsfile_tobq >> update_bq_table >> archive_srcfile >> insert_audit_entry

            if environment != "prod":

                if done_file == "None":
            #setting dag dependency
                    start_job >> [table_grp] >> end_job
                else:
                    start_job >> file_sensor >> delete_old_done_files >> [table_grp] >> end_job
            else:
                if done_file == "None":
            #setting dag dependency
                    start_job >> copy_data_task >> [table_grp] >> end_job
                else:
                    start_job >> file_sensor >> copy_data_task >> delete_old_done_files >> [table_grp] >> end_job
        return dag

config_kronos = cu.call_config_yaml("hrg_kronos_ingest_dependency.yaml","hca_hrg_kronos_ingest_dependency")
for ingest in config_kronos['ingest']:
    frequency = ingest["frequency"]
    schedule = ingest["schedule"]
    start_date = ingest["start_date"]
    source_system = ingest["source_system"]
    type = ingest["type"]
    dependency = ingest["dependency"]
    done_file = ingest["done_file"]

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
    globals()[dag_id] = create_dag(dag_id, schedule, start_date, dependency,source_system,frequency,done_file)