from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.api.common.experimental import get_task_instance

import json
import csv
import yaml
import gzip
import os
import sys
import glob
import re
import logging
import subprocess
import uuid
import gcsfs
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

import pandas as pd
import numpy as np
import pandas_gbq
from datetime import datetime, timedelta
import pendulum
import time

from paramiko import SSHClient
import paramiko
import shutil

logging.basicConfig(level=logging.INFO)

lob = "edwclm"
util_dir = os.path.dirname(__file__)
sys.path.append(util_dir)


def call_config_yaml(filename, variablename):
    CONFIG_FILE = filename
    cwd = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(cwd)
    config_folder = base_dir + "/config/"
    cfgfile = open(config_folder + CONFIG_FILE)
    default_config = yaml.safe_load(cfgfile)
    default_config = json.loads(json.dumps(default_config, default=str))
    config = Variable.setdefault(variablename, default_config,
                                 deserialize_json=True, description="Saved in dags/config/ folder")
    # config = default_config
    return config

config = call_config_yaml("edwclm_config.yaml","hca_clm_default_vars")

# get project variables from config file
bq_project_id = config['env']['v_curated_project_id']
bq_staging_dataset = config['env']['v_clm_stage_dataset_name']
stage_dataset = config['env']['v_clm_stage_dataset_name']
gcs_bucket = config['env']['v_data_bucket_name']
src_folder = config['env']['v_srcfilesdir']
arc_folder = config['env']['v_archivedir']
tgt_folder = config['env']['v_tgtfilesdir']
tmp_folder = config['env']['v_tmpobjdir']
schema_folder = tmp_folder + config['env']['v_schemasubdir']

current_timezone = pendulum.timezone("US/Central")
utc_timezone = pendulum.timezone('UTC')

def is_third_weekend() -> bool:
    '''
    Checks if the current date is either the 3rd Saturday of the month, or the Sunday after the 3rd Saturday
        Parameters:
            None
        
        Returns:
            is_third_weekend (bool): True if it is 3rd Weekend of the month, else False
    '''
    today = pendulum.now(current_timezone)
    first_day_of_the_month = today.start_of('month')
    first_saturday = first_day_of_the_month.next(pendulum.SATURDAY)
    third_saturday = first_saturday.add(weeks=2)
    if today.is_same_day(third_saturday) or today.is_same_day(third_saturday.add(days=1)):
        return True
    else:
        return False

def get_time_elements(date_time_value):
    hour = int(date_time_value.hour)
    min = int(date_time_value.minute)
    return hour, min

def get_date_elements(date_time_value):
    year = int(date_time_value.year)
    month = int(date_time_value.month)
    date = int(date_time_value.day)
    return year, month, date

def get_date_time_elements(date_time_value):
    year, month, date = get_date_elements(date_time_value)
    hour, min = get_time_elements(date_time_value)
    return year, month, date, hour, min

def get_local_datetime(year,month,date,hour,min):
    utc_date = pendulum.datetime(year,month,date,hour,min,0)
    local_date = current_timezone.convert(utc_date)
    print('Local Date : ', local_date)
    year = int(local_date.year)
    month = int(local_date.month)
    date = int(local_date.day)
    min = int(local_date.minute)
    hour = int(local_date.hour)
    return pendulum.datetime(year,month,date,hour,min,0,tz=current_timezone)

def get_time_diff(starting_hour, starting_min, ending_hour, ending_minute):

    if starting_hour == ending_hour:
        hour_diff = 0
    elif starting_hour < ending_hour:
        hour_diff = ending_hour - starting_hour
    else:
        hour_diff = 24 - starting_hour + ending_hour

    if starting_min == ending_minute:
        min_diff = 0
    elif starting_min < ending_minute:
        min_diff = ending_minute - starting_min
    else:
        min_diff = 60 - starting_min + ending_minute
        hour_diff = hour_diff - 1

    return hour_diff, min_diff

def get_execution_date(execution_date, **kwargs):  
    print("Execution Date - ",execution_date)  
    exec_day = int(execution_date.strftime('%w'))
    schedule = kwargs["params"]["schedule"]
    schedule_day = schedule.split()[4]

    # Set schedule day from execution day of the week if there are multiple day of the weeks in the schedule for dependnet DAG
    if re.search(",",schedule_day):
        schedule_day=schedule_day.split(",")
        if str(exec_day) in schedule_day:
            schedule_day=exec_day
        else:
            logging.info("Dependant DAG is not scheduled to run today")
            raise SystemExit()
    
    # Set poke date for day of the week schedules for dependent DAG
    if schedule_day == "*":
        poke_date = execution_date
    elif exec_day < int(schedule_day):
        date_diff = 7-int(schedule_day)+exec_day
        poke_date = execution_date - timedelta(days=date_diff)        
    elif exec_day >= int(schedule_day):
        date_diff = exec_day-int(schedule_day)
        poke_date = execution_date - timedelta(days=date_diff)

    # Get frequency of the running DAG if DAG passes the key argument
    if "frequency" in kwargs["params"]:
        frequency = kwargs["params"]["frequency"]
    else:
        frequency = None

    if frequency is None or "hourly" not in frequency:
        # Process for non-hourly running DAGs
        scheduled_hour = int(schedule.split()[1])
        scheduled_min = int(schedule.split()[0])
        if "sensor_dag_frequency" in kwargs["params"]:
            sensor_dag_frequency = kwargs["params"]["sensor_dag_frequency"]
            if sensor_dag_frequency == "hourly":
                if frequency is not None and frequency == "daily":
                    poke_date = poke_date.add(days=1)
                # Get Poke Date if dependent DAG is hourly
                year, month, date, hour, min = get_date_time_elements(poke_date)
                poke_date_local = get_local_datetime(year,month,date,hour,min)
                year, month, date, hour, min = get_date_time_elements(poke_date_local)
                hour_diff, min_diff = get_time_diff(scheduled_hour, scheduled_min, hour, min)
                if "hour_interval" in kwargs["params"]:
                    hour_interval = kwargs["params"]["hour_interval"]
                else:
                    hour_interval = 0
                prev_poke_date_local = poke_date_local.subtract(hours=hour_interval + hour_diff, minutes=min_diff)
                year, month, date, hour, min = get_date_time_elements(prev_poke_date_local)
            elif sensor_dag_frequency == "daily":
                if frequency is not None and frequency == "monthly":
                    poke_date = poke_date.add(months=1)
                # Get Poke Date if dependent DAG is daily
                year, month, date, hour, min = get_date_time_elements(poke_date)
                poke_date_local = get_local_datetime(year, month, date, hour, min)
                year, month, date, hour, min = get_date_time_elements(poke_date_local)
                hour_diff, min_diff = get_time_diff(scheduled_hour, scheduled_min, hour, min)
                if frequency is None or frequency == "daily":
                    day_interval = 0
                else:
                    if "day_interval" in kwargs["params"]:
                        day_interval = kwargs["params"]["day_interval"]
                    else:
                        day_interval = 0
                prev_poke_date_local = poke_date_local.subtract(days=day_interval, hours=hour_diff, minutes=min_diff)
                year, month, date, hour, min = get_date_time_elements(prev_poke_date_local)
        else:
            year, month, date = get_date_elements(poke_date)
            hour = scheduled_hour
            min = scheduled_min
    else:
        poke_date_local = current_timezone.convert(poke_date)
        print('Local Date : ', poke_date_local)
        if "cycle_age" in kwargs["params"]:
            cycle_age = kwargs["params"]["cycle_age"]
            if cycle_age == "previous":
                schedule_hour = schedule.split()[1]
                # Compile set of scheduled hours based on schedule hour cron expression 
                hourset_regex = re.compile("[\,]+", re.IGNORECASE)
                result = hourset_regex.search(schedule_hour)
                if result is None:
                    hourrange_regex = re.compile("([0-9]+)-([0-9]+)", re.IGNORECASE)
                    result = hourrange_regex.match(schedule_hour)
                    if result is None:
                        hourrange_regex = re.compile("([0-9]+)", re.IGNORECASE)
                        result = hourrange_regex.match(schedule_hour)
                        if result is None:
                            start_hour = 0
                        else:
                            start_hour = int(result.groups()[0])
                        end_hour = 24
                    else:
                        start_hour = int(result.groups()[0])
                        end_hour = int(result.groups()[1]) + 1
                    
                    hourstep_regex = re.compile("[0-9\-\*]+/([0-9]+)", re.IGNORECASE)
                    result = hourstep_regex.match(schedule_hour)
                    if result is None:
                        step_hour = 1
                    else:
                        step_hour = int(result.groups()[0])
                    scheduled_hour_list = []
                    for hour in range(start_hour, end_hour, step_hour):
                        scheduled_hour_list.append(hour)
                else:
                    scheduled_hour_list = list(map(int,schedule_hour.split(',')))
                scheduled_hour_list.insert(0, scheduled_hour_list.pop())

                # Determine the Previous Poke Date
                poke_date_local_hour = int(poke_date_local.hour)
                poke_date_utc_offset = poke_date_local.utcoffset()
                poke_index = scheduled_hour_list.index(poke_date_local_hour)
                poke_date_local_hour_prev = scheduled_hour_list[poke_index-1]

                if poke_date_local_hour_prev > poke_date_local_hour:
                    prev_poke_hour_diff = 24 - poke_date_local_hour_prev + poke_date_local_hour
                else:
                    prev_poke_hour_diff = poke_date_local_hour - poke_date_local_hour_prev

                prev_date_date_local = poke_date_local.subtract(hours=prev_poke_hour_diff)

                prev_poke_date_utc_offset = prev_date_date_local.utcoffset()

                utc_offset_diff = prev_poke_date_utc_offset - poke_date_utc_offset

                prev_date_date_local = prev_date_date_local - utc_offset_diff

                year, month, date, hour, min = get_date_time_elements(prev_date_date_local)

            else:
                year, month, date, hour, min = get_date_time_elements(poke_date_local)
        else:
            year, month, date, hour, min = get_date_time_elements(poke_date_local)

    poke_date = pendulum.datetime(year,month,date,hour,min,0,tz=current_timezone)
    print(poke_date)
    return poke_date

def get_bucket(project_id, bucket_name):
    # Get name of bucket
    try:
        storage_client = storage.Client(project_id)
        bucket = storage_client.bucket(bucket_name)
        return bucket
    except:
        logging.info("Bucket {} is not found in project {}".format(
            bucket_name, project_id))
        raise SystemExit()

# Integrate validation Function
timezone = pendulum.timezone("US/Central")
cwd = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.dirname(cwd)
sys.path.insert(0, base_dir)

def executevalidationsqls(**kwargs):
        print(f"{kwargs}")
        execution_date = kwargs['execution_date'] 
        task_instance = get_task_instance.get_task_instance(kwargs["dag_id"],kwargs["task_id"],execution_date)
        print(f"{task_instance}")
        start_date = task_instance.start_date.astimezone(timezone).strftime("%Y-%m-%d %H:%M:%S.%f")
        end_date = kwargs['task_instance'].start_date.astimezone(timezone).strftime("%Y-%m-%d %H:%M:%S.%f")
        val= kwargs["task_id"]
        print(f"{start_date}")
        print(f"{end_date}")
        print(f"{val}")
        bucket = get_bucket(kwargs['config']['env']['v_curated_project_id'], kwargs['config']['env']['v_dag_bucket_name'])
        storage_client = storage.Client()
        source = kwargs["source"].lower()
        file_list = kwargs["file_list"]
        print("File List ", file_list)
        validationsql_folder = f"dags/{lob}/sql/validation_sql/integrate/{source}"
        
        bq_table = kwargs["bq_table"].lower()
        blobs = storage_client.list_blobs(
            bucket, prefix=validationsql_folder, delimiter=None)
        # pattern = re.compile(fr"{bq_table}(_\d+)?\.sql")
        # file_list = [a for a in blobs if pattern.search(a.name)]
        client = bigquery.Client(project = kwargs['config']['env']['v_curated_project_id'])
        if "NONE" not in file_list:
            for filename in file_list:                
                validation_sql_path = f"dags/{lob}/sql/validation_sql/integrate/{source}/{filename}" 
                bqsqlsqryfile = open(base_dir + validation_sql_path.replace(f'dags/{lob}',''))
                print(base_dir)
                print(validation_sql_path)
                bqsqlsqry = bqsqlsqryfile.read()

                # Loop through the dictionary and replace each key with its corresponding value
                for old_text, new_text in kwargs['replacements'].items():
                    bqsqlsqry = bqsqlsqry.replace(old_text, new_text)
                
                for param in ("{{ "," }}",'params.'):
                    bqsqlsqry = bqsqlsqry.replace(param, '')
                   

                logging.info("===Executing SQL : {}===".format(filename))
                logging.info(bqsqlsqry)
                logging.info(f"target table {bq_table} {start_date} {end_date} {kwargs['task_id']}")

                job_config = bigquery.QueryJobConfig(
                         query_parameters=[
                            bigquery.ScalarQueryParameter("p_tableload_start_time", "DATETIME", start_date),
                            bigquery.ScalarQueryParameter("p_tableload_end_time", "DATETIME", end_date),
                            bigquery.ScalarQueryParameter("p_job_name", "STRING", kwargs["task_id"]),
                            bigquery.ScalarQueryParameter("p_targettable_name", "STRING", bq_table),
                        ]
                        )
                query_job = client.query(bqsqlsqry, job_config=job_config)
                logging.info(query_job)
        else:
            logging.info(
                "===Executing generic validation SQL to capture runtime")

            generic_validation_sql = f"dags/{lob}/sql/validation_sql/integrate/generic_validation_sql_runtime_capture.sql" 
            bqsqlsqryfile = open(base_dir + generic_validation_sql.replace(f'dags/{lob}',''))
            bqsqlsqry = bqsqlsqryfile.read()

            # Loop through the dictionary and replace each key with its corresponding value
            for old_text, new_text in kwargs['replacements'].items():
                bqsqlsqry = bqsqlsqry.replace(old_text, new_text)
            
            for param in ("{{ "," }}",'params.'):
                    bqsqlsqry = bqsqlsqry.replace(param, '')
                    

            logging.info("===Executing SQL : {}===".format(generic_validation_sql))
            logging.info(bqsqlsqry)
            logging.info(f"target table {bq_table} {start_date} {end_date} {kwargs['task_id']}")

            job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                    bigquery.ScalarQueryParameter("p_tableload_start_time", "DATETIME", start_date),
                    bigquery.ScalarQueryParameter("p_tableload_end_time", "DATETIME", end_date),
                    bigquery.ScalarQueryParameter("p_job_name", "STRING", kwargs["task_id"]),
                    bigquery.ScalarQueryParameter("p_targettable_name", "STRING", bq_table),
                    bigquery.ScalarQueryParameter("p_source", "STRING", source)
                ]
                )
            query_job = client.query(bqsqlsqry, job_config=job_config)
            logging.info(query_job)

def delete_files_with_filename(**kwargs):
    gcs_bucket_name = kwargs['gcs_bucket_name']
    gcs_folder = kwargs['gcs_folder']
    gcs_project_id = kwargs['gcs_project_id']
    target_filename = kwargs['target_filename']
    current_date = pendulum.today().date()
    seven_days_ago = current_date.subtract(days=7)
    # logging.info(seven_days_ago)
    # logging.info(current_date)
    # logging.info(target_filename)
    # print(target_filename)

    # Setting up the client to interact with the GCS Bucket
    client = storage.Client(project=gcs_project_id)
    bucket = client.bucket(gcs_bucket_name)

    # Defining the prefix for the path
    prefix = gcs_folder + '/' if gcs_folder else ''
    logging.info(prefix)
    deleted_files_count = 0

    for blob in bucket.list_blobs(prefix=prefix):
        # logging.info("blobs ".format(blob))
        # if blob.name.endswith(target_filename):
        # logging.info("blob ".format(blob))
        # Extract the date part from the blob name
        date_part = blob.name.split('_')[-1].split('.')[0]
        # logging.info("date_part".format(date_part))
        
        try:
            file_date = pendulum.from_format(date_part, 'YYYYMMDD').date()
            # logging.info("try")
        except ValueError:
            continue
        # logging.info("file_date".format(file_date))
        # logging.info("seven_days_ago".format(seven_days_ago))
        if file_date < seven_days_ago:
            blob.delete()
            deleted_files_count += 1
            print(f"Deleted file: {blob.name}")

    logging.info("=================The Total files deleted: {}=================".format(deleted_files_count))

def delete_done_files(**kwargs):
    gcs_bucket_name = kwargs['gcs_bucket_name']
    gcs_folder = kwargs['gcs_folder']
    gcs_project_id = kwargs['gcs_project_id']
    done_file = kwargs['done_file']
    client = storage.Client(project=gcs_project_id)
    bucket = client.bucket(gcs_bucket_name)

    blob =bucket.blob(gcs_folder + '/'+ done_file)
    blob.delete()
    print(f"Deleted file: {blob.name}")

def removegcsfileifexists(sourcesysname, folder, filename):
    logging.info("===== In removegcsfile params: =====")
    logging.info("Source System : {}, Folder : {}, File Name : {}".format(
        sourcesysname, folder, filename))
    bucket = get_bucket(bq_project_id, gcs_bucket)
    objectfullpath = '{}{}/{}'.format(folder, sourcesysname, filename)
    blob = bucket.blob(objectfullpath)
    if blob.exists():
        blob.delete()
        logging.info("Source file {}  in GCS removed".format(blob.name))
    else:
        logging.info("Source file {}  in not found".format(blob.name))
    return None

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

def sftp_get_files(sourcesysname, remote_directory,file_pattern,pwd_secret, host, user, port):
        logging.info("==== In sftp_get_files with params: ====")
        logging.info("Source System : {}, Remote Directory : {}, File Pattern : {}, Pwd Secret : {}, host: {}, User:{}, Port:{}".format(sourcesysname, remote_directory,file_pattern,pwd_secret, host, user, port))
        ssh_client = paramiko.SSHClient()
        ssh_client.load_system_host_keys()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        password=access_secret(pwd_secret)
        print("PWD : ",password)
        ssh_client.connect(hostname=host, port=port,username=user,password=password, banner_timeout=3600)
        print("connection successful")
        #open sftp
        ftp_client=ssh_client.open_sftp()
        print("SFTP opened")
        file_details = {}
        try:
            listing = ftp_client.listdir()
            print("Directory List",listing)
            print("FIles in Directory",ftp_client.listdir(remote_directory))
            file_details = {}
            load_start_time = pendulum.now('US/Central').to_datetime_string()
            patterns_found = []
            for pattern in file_pattern:
                src_name = re.sub(r"[^a-zA-Z0-9_]+", '', pattern.get('name').split('.')[-2])
                file_details[src_name] = {}                
                for file_name in ftp_client.listdir(remote_directory):
                    is_pattern = re.search(pattern.get('name'), file_name)
                    if is_pattern:
                        print("File matching regex",file_name)
                        remotelocation = remote_directory+file_name
                        filelocation = '/home/airflow/gcs/data/'+file_name
                        print(filelocation)
                        #ftp_client.get(remotelocation, filelocation)                        
                        sftp_file_instance = ftp_client.open(remotelocation, 'r')
                        with open(filelocation, 'wb') as fo:
                            shutil.copyfileobj(sftp_file_instance, fo)                  
                        file_line_count = int(subprocess.check_output(['wc','-l', filelocation]).split()[0])
                        # minus 1 to remove header from csv file to get actual records
                        file_line_count -= 1
                        print(f"File Name {filelocation}, Record count {file_line_count}")
                        file_details[src_name][file_name] = file_line_count
                        #file_details.append({src_name: {file: file_line_count}})
                        upload_object_from_file(sourcesysname, src_folder, file_name, filelocation)
                        removelocalfile(filelocation)
                        if pattern.get('name') not in patterns_found:
                            patterns_found.append(pattern.get('name'))                        
            print(f"below file patterns are found on sftp server.... {patterns_found} while searching for {file_pattern}")
            if len(patterns_found) != len(file_pattern):
                print(f"few file patterns cant be found on sftp server. failing... {patterns_found}")
                raise AirflowFailException                            
        except Exception as e:
            print("error",e)
            raise AirflowFailException
        ftp_client.close()
        print("Connection Closed!")
        return file_details, load_start_time

def insert_audit_data_files(**kwargs):
    """
    This method is used to insert audit data into audit_control table by getting the record count for the source file and staging table
    """
    print("Start_Audit_Data")
    sourcesysname = kwargs['sourcesysname']
    dag_id = kwargs['dag_id']
    source_id = kwargs['source_id']
    # source_file_name_prefix = kwargs['source_file_name_prefix']
    # source_file_extension = kwargs['source_file_extension']
    encoding_scheme = kwargs['encoding_scheme']
    tolerance_percent = kwargs['tolerance_percent']
    # source_file_name = f'{source_file_name_prefix}.{source_file_extension}'
    source_file_name = kwargs['source_file_name']
    bucket = get_bucket(kwargs['bq_project_id'], kwargs['gcs_bucket'])
    blob = get_blob(bucket, kwargs['src_folder'], sourcesysname, source_file_name)
    blob.download_to_filename(source_file_name)
    expected_value = sum(1 for _ in enumerate(open(source_file_name, encoding=encoding_scheme)))
    print("Expected Count : ", expected_value)

    bq_staging_dataset = kwargs['bq_staging_dataset']
    target = kwargs['tgt_bq_table']
	
    load_start_time = kwargs['load_start_time']
    load_end_time = pendulum.now('US/Central')
    
    bq_client = bigquery.Client(project=kwargs['config']['env']['v_curated_project_id'])
    target_table = kwargs['bq_staging_dataset'] + '.' + kwargs['tgt_bq_table']        
    result = bq_client.query(f'select count(*) as rec_count from {target_table}').result()
    
    actual_value = 0
    for rec in result:
        actual_value =  rec["rec_count"]
    print("Actual Count : ", actual_value)

    if expected_value == actual_value:
        audit_status = 'PASS'
    elif actual_value > expected_value:
        audit_status = 'PASS(More records in Target)'
    else:
        if tolerance_percent != 0 and expected_value != 0:
            var_pct = round((abs(actual_value-expected_value)/expected_value) * 100,2)
            print("Variance Percentage ", var_pct)
            if var_pct > tolerance_percent:
                audit_status = 'FAIL'
            else:
                audit_status = 'PASS(Less records in target but meets tolerance)'
        else:
            audit_status = 'FAIL'

    audit_records = []
    audit_records.append({'uuid' : str(uuid.uuid4()),
                    'table_id': source_id,
                    'src_sys_nm': sourcesysname,
                    'src_tbl_nm': source_file_name,
                    'tgt_tbl_nm': target_table,
                    'audit_type': 'RECORD_COUNT',
                    'expected_value':expected_value,
                    'actual_value': actual_value,
                    'load_start_time': load_start_time,
                    'load_end_time': load_end_time.to_datetime_string(),
                    'load_run_time': str(datetime.fromisoformat(load_end_time.to_datetime_string()) - datetime.fromisoformat(load_start_time)),
                    'job_name': dag_id + '-' + pendulum.now(timezone).strftime("%Y%m%d%H%M%S"),
                    'audit_time': str(pendulum.now(timezone))[:23],
                    'audit_status': audit_status })

    print(f"audit records are {audit_records}")
    audit_table = bq_client.dataset(kwargs['config']['env']['v_clm_audit_dataset_name']).table(kwargs['config']['env']['v_audittablename'].split('.')[-1])
    audit_table_ref = bq_client.get_table(audit_table)
    audit_result = bq_client.insert_rows(audit_table_ref,audit_records)
    print(f"result of inserting into audit is {audit_result}")

def access_secret(secret_resourceid):
    from google.cloud import secretmanager
    client = secretmanager.SecretManagerServiceClient()
    payload = client.access_secret_version(name=secret_resourceid).payload.data.decode("UTF-8")
    return payload