from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.api.common.experimental import get_task_instance
from google.api_core.exceptions import TooManyRequests

import json
import csv
import yaml
import gzip
import os
import sys
import glob
import re
import io
import logging
import subprocess
import uuid
import math
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
                                 deserialize_json=True, description="Saved in dags/edwra/config/ folder")
    # config = default_config
    return config

config = call_config_yaml("ra_config.yaml","hca_ra_default_vars")

# # get project variables from config file
bq_project_id = config['env']['v_curated_project_id']
bq_staging_dataset = config['env']['v_parallon_ra_stage_dataset_name']
gcs_bucket = config['env']['v_data_bucket_name']
src_folder = config['env']['v_srcfilesdir']
# arc_folder = config['env']['v_archivedir']
# tgt_folder = config['env']['v_tgtfilesdir']
# tmp_folder = config['env']['v_tmpobjdir']
# schema_folder = tmp_folder + config['env']['v_schemasubdir']
timezone_str = "US/Central"
current_timezone = pendulum.timezone(timezone_str)
utc_timezone = pendulum.timezone('UTC')

def get_execution_date(execution_date, **kwargs):  
    print("Execution Date - ",execution_date)  
    exec_day = int(execution_date.strftime('%w'))
    print("Execution Day - ",exec_day)
    schedule = kwargs["params"]["schedule"]
    print("Sensor Schedule - ", schedule)
    schedule_day = schedule.split()[4]
    if re.search(",",schedule_day):
        schedule_day=schedule_day.split(",")
        if str(exec_day) in schedule_day:
            schedule_day=exec_day
        else:
            logging.info("Dependant DAG is not scheduled to run today")
            raise SystemExit() 
    if schedule_day == "*" and "frequency" in kwargs["params"] and kwargs["params"]["frequency"] in ['weekly', 'monthly']:
        poke_date = datetime.now() - timedelta(1)
    elif schedule_day == "*":
        poke_date = execution_date
    elif exec_day < int(schedule_day):
        date_diff = 7-int(schedule_day)+exec_day
        print(date_diff)
        print(execution_date - timedelta(days=date_diff))
        poke_date = execution_date - timedelta(days=date_diff)        
    elif exec_day >= int(schedule_day):
        date_diff = exec_day-int(schedule_day)
        print(execution_date - timedelta(days=date_diff))
        poke_date = execution_date - timedelta(days=date_diff)
    print(poke_date.year)

    if "frequency" in kwargs["params"]:
        frequency = kwargs["params"]["frequency"]
        if "hourly" in frequency:
            execution_date_local = current_timezone.convert(execution_date)
            print(execution_date_local)
            if "cycle_age" in kwargs["params"]:
                cycle_age = kwargs["params"]["cycle_age"]
                if cycle_age == "previous":
                    schedule_hour = schedule.split()[1]
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

                    print(scheduled_hour_list)
                    execution_date_local_hour = int(execution_date_local.hour)
                    execution_index = scheduled_hour_list.index(execution_date_local_hour)
                    execution_date_local_hour_prev = scheduled_hour_list[execution_index-1]

                    if execution_date_local_hour_prev > execution_date_local_hour:
                        prev_execution_hour_diff = 24 - execution_date_local_hour_prev + execution_date_local_hour
                    else:
                        prev_execution_hour_diff = execution_date_local_hour - execution_date_local_hour_prev

                    prev_execution_date_local = execution_date_local.subtract(hours=prev_execution_hour_diff)

                    year = int(prev_execution_date_local.year)
                    month = int(prev_execution_date_local.month)
                    date = int(prev_execution_date_local.day)
                    min = int(prev_execution_date_local.minute)
                    hour = int(prev_execution_date_local.hour)
                else:
                    year = int(execution_date_local.year)
                    month = int(execution_date_local.month)
                    date = int(execution_date_local.day)
                    min = int(execution_date_local.minute)
                    hour = int(execution_date_local.hour)                    
            else:
                year = int(execution_date_local.year)
                month = int(execution_date_local.month)
                date = int(execution_date_local.day)
                min = int(execution_date_local.minute)
                hour = int(execution_date_local.hour)
        elif "daily" in frequency:
            poke_date = poke_date - timedelta(days=1)
            year = int(poke_date.year)
            month = int(poke_date.month)
            date = int(poke_date.day)
            min = int(schedule.split()[0])
            hour = int(schedule.split()[1])
        else:
            year = int(poke_date.year)
            month = int(poke_date.month)
            date = int(poke_date.day)
            min = int(schedule.split()[0])
            hour = int(schedule.split()[1])
    else:
        year = int(poke_date.year)
        month = int(poke_date.month)
        date = int(poke_date.day)
        min = int(schedule.split()[0])
        hour = int(schedule.split()[1])
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
        val = kwargs["task_id"]
        print(f"{start_date}") 
        print(f"{end_date}")
        print(f"{val}")      
        bucket = get_bucket(kwargs['config']['env']['v_proc_project_id'], kwargs['config']['env']['v_dag_bucket_name'])
        storage_client = storage.Client()
        source = kwargs["source"].lower()
        validationsql_folder = "dags/" + kwargs['config']['env']['v_lob'] + "/sql/validation_sql/integrate/"
        bq_table = kwargs["bq_table"].lower()
        blobs = storage_client.list_blobs(
            bucket, prefix=validationsql_folder, delimiter=None)
        pattern = re.compile(fr"{bq_table}_exp(_\d+)?\.sql")
        file_list = [a for a in blobs if pattern.search(a.name)]
        client = bigquery.Client(project=kwargs['config']['env']['v_curated_project_id'])
        if file_list:
            for filename in file_list:
                bqsqlsqryfile = open(base_dir + filename.name.replace('dags/' + kwargs['config']['env']['v_lob'],''))
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
                "===Did not find any Validation SQL for table - {}===".format(bq_table))
            logging.info(
                "===Executing generic validation SQL to capture runtime")

            generic_validation_sql = "dags/" + kwargs['config']['env']['v_lob'] + "/sql/validation_sql/integrate/" + "generic_validation_sql_runtime_capture.sql"
            bqsqlsqryfile = open(base_dir + generic_validation_sql.replace('dags/' + kwargs['config']['env']['v_lob'],''))
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

    blob = bucket.blob(gcs_folder + '/' + done_file)
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
        try:
            blob.delete()
            logging.info("Source file {}  in GCS removed".format(blob.name))
        except NotFound:
            logging.info("Due to sharing of done files, this file was deleted through another process")
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
    audit_table = bq_client.dataset(kwargs['config']['env']['v_cr_audit_dataset_name']).table(kwargs['config']['env']['v_audittablename'].split('.')[-1])
    audit_table_ref = bq_client.get_table(audit_table)
    audit_result = bq_client.insert_rows(audit_table_ref,audit_records)
    print(f"result of inserting into audit is {audit_result}")

def write_file_header(bucket, header_name, extract_file_type):
    if extract_file_type == 'denials':
        header_content = 'ssc_name|facility_name|coid|unit_num|rate_schedule_name|rate_schedule_eff_begin_date|rate_schedule_eff_end_date|account_no|patient_name|patient_dob|iplan_id|insurance_provider_name|payer_group_name|billing_name|billing_contact_person|authorization_code|payer_patient_id|payer_rank|pa_financial_class|payor_financial_class|accounting_period|major_payer_grp|reason_code|billing_status|pa_service_code|pa_account_status|cc_patient_type|pa_discharge_status|pa_patient_type|cancel_bill_ind|admit_source|admit_type|pa_drg|attending_physician_id|attending_physician_name|service_date_begin|discharge_date|comparison_method|project_name|work_queue_name|status_category_desc|status_desc|status_phase_desc|calc_date|total_charges|pa_actual_los|total_billed_charges|covered_charges|total_expected_payment|total_expected_adjustment|total_pt_responsibility_actual|total_variance_adjustment|total_payments|total_denial_amt|payor_due_amt|pa_total_account_bal|ar_amount|otd_to_date_amount|max_aplno|max_seqno|appeal_orig_amt|current_appealed_amt|current_appeal_balance|appeal_date_created|sequence_date_created|close_date|max_seq_deadline_date|sequence_creator|appeal_owner|appeal_modifier|disp_code|disp_desc|web_disp_code|root_code|root_cause_description|root_cause_dtl|external_appeal_code|apl_appeal_code|apl_appeal_desc|first_denial_date|denial_age|pa_denial_update_date|first_activity_create_date|last_activity_completion_date|last_activity_completion_age|last_user_activity_create_age|last_reason_change_date|last_status_change_date|last_project_change_date|last_owner_change_date|activity_due_date|activity_desc|activity_subject|activity_owner|appeal_sent_activity_ownr|appeal_sent_activity_date|appeal_sent_activity_age|appeal_sent_activity_subj|appeal_sent_activity_desc|stratification|status_kpi|followup_kpi|appealsent_kpi|deadline_kpi|mon_account_payer_id|extract_date|payer_category|writeoff_amt|cash_adj_amt\n'
    elif extract_file_type == 'inventory':
        header_content = 'ssc_name|facility_name|coid|unit_num|rate_schedule_name|rate_schedule_eff_begin_date|rate_schedule_eff_end_date|account_no|patient_name|patient_dob|iplan_id|insurance_provider_name|payer_group_name|billing_name|billing_contact_person|authorization_code|payer_patient_id|payer_rank|pa_financial_class|payor_financial_class|reason_code|billing_status|pa_service_code|pa_account_status|cc_patient_type|pa_discharge_status|pa_patient_type|cancel_bill_ind|admit_source|pa_drg|attending_physician_id|attending_physician_name|service_date_begin|discharge_date|comparison_method|project_name|work_queue_name|status_category_desc|status_desc|status_phase_desc|calc_date|overpayment_date|overpayment_age|underpayment_date|underpayment_age|non_fin_disc_date|non_fin_disc_age|variance_creation_date|variance_creation_age|variance_resolution_date|variance_resolution_age|first_activity_create_date|last_activity_completion_date|last_activity_completion_age|last_reason_change_date|last_status_change_date|last_project_change_date|last_owner_change_date|activity_due_date|activity_desc|activity_subject|activity_owner|total_charges|pa_actual_los|total_billed_charges|total_expected_payment|total_expected_adjustment|total_pt_responsibility_actual|total_variance_adjustment|total_payments|total_denial_amt|payor_due_amt|ar_amount|pa_total_account_bal|user_completed_activity_date|user_completed_activity_age|user_completed_activity_subj|user_completed_activity_desc|user_completed_activity_ownr|valid_overpymnt_activity_date|valid_overpymnt_activity_age|valid_overpymnt_activity_subj|valid_overpymnt_activity_desc|valid_overpymnt_activity_ownr|valid_underpymnt_activity_date|valid_underpymnt_activity_age|valid_underpymnt_activity_subj|valid_underpymnt_activity_desc|valid_underpymnt_activity_ownr|stratification|status_kpi|validation_grp|discrepancy_group|credit|payer_category|model_issue|credit_balance_age|activity_kpi|extract_date|schema_id\n'
    else: 
        logging.error('Header definition is not defined and cannot be created')
        raise SystemExit()

    blob = bucket.blob(header_name)
    blob.upload_from_string(header_content)
    logging.info(f"File with name {header_name} was uploaded to bucket {bucket.id}")


def combine_chunked_files(bucket_name, ssc_name, outbound_file_path, extract_file_type, bq_suffix):
    bucket = storage.Client().bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=f'{outbound_file_path}chunk_{extract_file_type}_{ssc_name}_')
    blobs_list = list(blobs)
    logging.info(f'{len(blobs_list)} chunked files were found to be combined')
    max_elements = 30
    # Breaks list into chunks < 32 (Limit for compose function is 32)
    blob_list_chunks = [blobs_list[i * max_elements:(i + 1) * max_elements] for i in range((len(blobs_list) + max_elements - 1) // max_elements )]

    destination = bucket.blob(f'{outbound_file_path}{ssc_name}{extract_file_type.title()}{bq_suffix.upper()}.txt')
    header_name = f'{outbound_file_path}{extract_file_type}_header.txt'
    if storage.Blob(bucket=bucket, name=header_name).exists():
        pass
    else:
        logging.info(f"Header for file does not exist at location {header_name}, uploading to GCS bucket")
        write_file_header(bucket, header_name, extract_file_type)

    for idx, blob_chunk in enumerate(blob_list_chunks):
        logging.info(idx)
        logging.info(len(blob_chunk))
        if idx == 0:
            header_blob = bucket.get_blob(header_name)
            blob_chunk.insert(0, header_blob)
        else:
            destination_blob = bucket.get_blob(f'{outbound_file_path}{ssc_name}{extract_file_type.title()}{bq_suffix.upper()}.txt')
            blob_chunk.insert(0, destination_blob)
        attempt_limit = 5
        for attempt_no in range(attempt_limit):
            try:
                destination.compose(blob_chunk)
                break
            except TooManyRequests:
                logging.info("Hitting rate limit. Waiting before retrying")
                wait_time = (math.ceil(2.5 ** attempt_no))
                time.sleep(wait_time)
            except Exception:
                raise
    logging.info(f'Completed combination of chunked files into file {destination.name}')
    
    for blob in blobs_list:
        blob.delete()
    logging.info('Completed delete of chunked files')

def rename_blob(bucket_name, path_to_blob, blob_name, new_blob_name):
    bucket = storage.Client().get_bucket(bucket_name)
    bucket.rename_blob(bucket.get_blob(path_to_blob + blob_name), path_to_blob + new_blob_name)
    logging.info(f'Renamed blob {blob_name} to {new_blob_name} in bucket {bucket_name}')

def get_edw_etl_load_bq_field(col_expression: str, table_name: str, src_schema_id) -> str:
    '''Query BQ table to get column value to insert into query'''
    query_str = "SELECT {} as val FROM {}.edw_etl_load WHERE Schema_Id = {} and lower(table_name) = LOWER('{}');".format(col_expression, bq_staging_dataset, str(src_schema_id), table_name)
    logging.info("===Running query===")
    logging.info(str(query_str))
    res_df = pd.read_gbq(query_str, project_id=bq_project_id)
    logging.info("===Done running query===")
    bq_field = res_df['val'][0]
    return bq_field

def get_day_of_week() -> int:
    return int(datetime.now().strftime('%w'))

def replace_edw_etl_load_field(sql: str, table_name: str, src_schema_id) -> str:
    '''Replaces references to HCA_REPORTING.EDW_ETL_LOAD table with the output of that query from the BQ table'''
    logging.info("Replacing edw_etl_load fields")
    # Regex pattern to find subqueries referencing EDW_ETL_LOAD table
    pattern = r"\(\s*SELECT\s+([^()]*?)\s+FROM\s+[^()]*?\s+WHERE[^()]*?TABLE_NAME\s*=\s*'([^']*)'.*?\)"
    def replace_re(match):
        col_expression = match.group(1)
        # Sometimes table has alias, remove from field name select if that is the case
        col_expression = col_expression.split('.')[-1]
        # Sometimes value for table_name filter is a env variable ($) instead of string, handle both cases
        table_name_to_query = table_name if '$' in match.group(2) else match.group(2)
        bq_val = get_edw_etl_load_bq_field(col_expression, table_name_to_query, src_schema_id)
        oracle_val_string = "TO_DATE('{}', 'YYYY-MM-DD HH24:MI:SS')".format(bq_val)
        return oracle_val_string
    # Replace all occurrences of that pattern with the corresponding value from the BQ EDW_ETL_LOAD table
    replaced_sql = re.sub(pattern, replace_re, sql, flags=re.IGNORECASE)
    return replaced_sql

def oracle_preprocess_update(table_to_load: str, src_schema_id):
    bq_client = bigquery.Client(bq_project_id)
    logging.info(f"Beginning edw_etl_load table pre processing update for {table_to_load}")
    for attempt in range(1, 8):
        try:
            update_sql_pre = f'''UPDATE `{bq_staging_dataset}.edw_etl_load`
            SET CURRENT_EFF_TO_DATE_TIME = DATE_SUB(DATETIME_TRUNC(CURRENT_DATETIME('{timezone_str}'), SECOND), INTERVAL 5 MINUTE),
            CURRENT_EFF_FROM_DATE_TIME = LAST_EFF_TO_DATE_TIME,
            LAST_UPDATE_DATE_TIME = DATETIME_TRUNC(CURRENT_DATETIME('{timezone_str}'), SECOND)
            WHERE SCHEMA_ID = {src_schema_id} AND lower(TABLE_NAME) = lower('{table_to_load}');
            '''
            bq_client.query(update_sql_pre).result()
            logging.info(f"Completed edw_etl_load table pre processing update for {table_to_load}")
            break
        except Exception as e:
            logging.info("edw_etl_load update failed due to too many concurrent updates, retrying")
            wait_time = (math.ceil(2.5 ** attempt))
            time.sleep(wait_time)

def oracle_postprocess_update(bq_client, table_to_load: str, src_schema_id):
    logging.info(f"Beginning edw_etl_load table post processing update for {table_to_load}")
    for attempt in range(1, 8):
        try:
            update_sql_post = f'''UPDATE `{bq_staging_dataset}.edw_etl_load`
            SET LAST_EFF_FROM_DATE_TIME = CURRENT_EFF_FROM_DATE_TIME,
            LAST_EFF_TO_DATE_TIME = CURRENT_EFF_TO_DATE_TIME,
            Last_Update_Date_Time = DATETIME_TRUNC(CURRENT_DATETIME('{timezone_str}'), SECOND)
            WHERE SCHEMA_ID = {src_schema_id} AND lower(TABLE_NAME) = lower('{table_to_load}');
            '''
            bq_client.query(update_sql_post).result()
            logging.info("Completed edw_etl_load table post processing update")
            break
        except Exception as e:
            logging.info("edw_etl_load update failed due to too many concurrent updates, retrying")
            wait_time = (math.ceil(2.5 ** attempt))
            time.sleep(wait_time)

def purge_calc_data(bq_client, delete_from_table: str, insert_to_table: str, comparison_table: str, alt_id_field: str, id_field: str, src_schema_id):
    '''Generate and execute calc delete statements. Replaces multiple custom .sql files with one generic function that can be used in multiple jobs.'''
    purge_old_calcs_sql = f'''INSERT INTO {bq_staging_dataset}.{insert_to_table}
    SELECT * FROM {bq_staging_dataset}.{delete_from_table} A
    WHERE NOT EXISTS ( SELECT 1
                    FROM {bq_staging_dataset}.{comparison_table} B
                    WHERE A.{alt_id_field} = B.{id_field} AND
                            A.Schema_Id = B.Schema_Id )
    AND A.Schema_Id = {src_schema_id};

    DELETE FROM {bq_staging_dataset}.{delete_from_table} A
    WHERE NOT EXISTS ( SELECT 1 
                    FROM {bq_staging_dataset}.{comparison_table} B
                    WHERE A.{alt_id_field} = B.{id_field}
                    AND A.Schema_ID = B.Schema_ID) 
    AND A.Schema_Id = {src_schema_id};'''
    logging.info("Beginning calc purge")
    logging.info(f"Running query {purge_old_calcs_sql}")
    bq_client.query(purge_old_calcs_sql).result()
    logging.info("Finished calc purge")

def purge_calc_latest_data(bq_client, delete_from_table: str, insert_to_table: str, comparison_table: str, alt_id_field: str, id_field: str, src_schema_id):
    '''Generate and execute calc delete statements. Replaces multiple custom .sql files with one generic function that can be used in multiple jobs.'''
    purge_old_calcs_sql = f'''INSERT INTO {bq_staging_dataset}.{insert_to_table}
    SELECT * FROM {bq_staging_dataset}.{delete_from_table} A
    WHERE NOT EXISTS ( SELECT 1
                    FROM {bq_staging_dataset}.{comparison_table} B
                    WHERE A.{alt_id_field} = B.{id_field} AND
                            A.Schema_Id = B.Schema_Id )
    AND A.Schema_Id = {src_schema_id};

    DELETE FROM {bq_staging_dataset}.{delete_from_table} A
    WHERE NOT EXISTS ( SELECT 1 
                    FROM {bq_staging_dataset}.{comparison_table} B
                    WHERE A.{alt_id_field} = B.{id_field}
                    AND A.Schema_ID = B.Schema_ID) 
    AND A.Schema_Id = {src_schema_id};'''
    logging.info("Beginning calc purge")
    logging.info(f"Running query {purge_old_calcs_sql}")
    bq_client.query(purge_old_calcs_sql).result()
    logging.info("Finished calc purge")

def mon_account_nonclinical_code_postprocessing(bq_client, src_schema_id):
    del_table_id = f"{bq_staging_dataset}.mon_account_nonclinical_code_del"
    delete_sql = f'''DELETE FROM {bq_staging_dataset}.mon_account_nonclinical_code_c AS a WHERE EXISTS (
    SELECT 1
    FROM {del_table_id} AS b
    WHERE a.id = b.id
    AND a.schema_id = b.schema_id
    AND b.schema_id = {src_schema_id}
    );'''
    logging.info('Deleting from mon_account_nonclinical_code_c where in del')
    bq_client.query(delete_sql).result()

def ssc_denials_cleanup(bq_client, src_schema_id):
    delete_query = f'''DELETE FROM {bq_staging_dataset}.ssc_denials WHERE (ssc_denials.schema_id, ssc_denials.ssc_id, upper(ssc_denials.coid), ssc_denials.mon_account_payer_id) IN(
    SELECT AS STRUCT
        schema_id,
        ssc_id,
        coid,
        mon_account_payer_id
        FROM
        {bq_staging_dataset}.ssc_denials
        GROUP BY 1, 2, 3, 4
        HAVING count(*) > 1
    )
    AND pass_type = '2'
    AND ssc_denials.schema_id = {src_schema_id};'''
    bq_client.query(delete_query).result()
    logging.info(f"Deleting duplicates from {bq_staging_dataset}.ssc_denials where schema_id = {src_schema_id}")

def is_cdc_full_pull(table_to_load: str, src_schema_id) -> bool:
    '''Queries CDC_IND table to determine whether a full pull should be done'''
    # return False # For testing purposes
    sql = "SELECT * FROM {}.cdc_ind WHERE schema_id = {} and LOWER(table_name) = LOWER('{}')".format(bq_staging_dataset, src_schema_id, table_to_load)
    logging.info("Running sql to check CDC_IND")
    logging.info(sql)
    try:
        res = pd.read_gbq(sql, project_id=bq_project_id)['cdc_ind'][0]
        if res == 1:
            logging.info("CDC_IND is 1, do CDC pull")
            return False
        else:
            logging.info("CDC_IND is not 1, do full pull")
            return True
    except IndexError as e:
        logging.info("Table {} not found in {}.cdc_ind".format(table_to_load, bq_staging_dataset))
        return False

def construct_merge_statement(bq_client, primary_keys: list, target_table: str, source_table: str, src_schema_id) -> str:
    '''Creates a MERGE DML query in BQ-compliant syntax'''
    table = bq_client.get_table(target_table)
    f = io.StringIO("")
    bq_client.schema_to_json(table.schema, f)
    tblschema = json.loads(f.getvalue())

    merge_str = f"MERGE {target_table} T \nUSING (SELECT * FROM {source_table} WHERE schema_id = {src_schema_id}) S\nON "
    update_fields = [col['name'] for col in tblschema if col['name'] not in primary_keys]
    for index, primary_key in enumerate(primary_keys):
        if index == 0:
            merge_str += "T.{} = S.{}\n".format(primary_key, primary_key)
        else:
            merge_str += "AND T.{} = S.{}\n".format(primary_key, primary_key)
    merge_str += "WHEN MATCHED THEN UPDATE SET\n"
    for index, col in enumerate(update_fields):
        if index == 0:
            merge_str += "T.{} = S.{}".format(col, col)
        else:
            merge_str += ",\nT.{} = S.{}".format(col, col)
    merge_str += "\nWHEN NOT MATCHED THEN\nINSERT ROW"
    return merge_str

def post_processing_deletes(bq_client, table_to_load, src_schema_id):
    if table_to_load == 'mon_account_payer_calc_apc':
        purge_calc_data(bq_client, 'mon_account_payer_calc_apc', 'mon_account_payer_calc_apc_old', 'mon_account_payer_calc_latest', 'mon_acct_payer_calc_summary_id', 'id', src_schema_id)
    elif table_to_load == 'mon_account_payer_apc_comp_dtl':
        purge_calc_data(bq_client, 'mon_account_payer_apc_comp_dtl', 'mon_acct_pyr_apc_comp_dtl_old', 'mon_account_payer_calc_latest', 'mon_acct_payer_calc_summary_id', 'id', src_schema_id)
    elif table_to_load == 'mon_account_payer_calc_coin_fs':
        purge_calc_data(bq_client, 'mon_account_payer_calc_coin_fs', 'mon_acct_pyr_calc_coin_fs_old', 'mon_account_payer_calc_latest', 'mon_acct_payer_calc_summary_id', 'id', src_schema_id)
    elif table_to_load == 'mon_account_payer_calc_fs':
        purge_calc_data(bq_client, 'mon_account_payer_calc_fs', 'mon_account_payer_calc_fs_old', 'mon_account_payer_calc_latest', 'mon_acct_payer_calc_summary_id', 'id', src_schema_id)
    elif table_to_load == 'apg_calc_output':
        purge_calc_data(bq_client, 'apg_calc_output', 'apg_calc_output_old', 'mon_account_payer_calc_latest', 'mon_acct_payer_calc_summary_id', 'id', src_schema_id)
    elif table_to_load == 'mon_account_nonclinical_code_c' and not(is_cdc_full_pull(table_to_load, src_schema_id)):
        mon_account_nonclinical_code_postprocessing(bq_client, src_schema_id)
    elif table_to_load == 'ssc_denials':
        ssc_denials_cleanup(bq_client, src_schema_id)


def oracle_ingest_post_processing(table_infos: list, src_schema_id, tables_to_ignore: list):
    src_schema_id = str(src_schema_id)
    bq_client = bigquery.Client(bq_project_id)

    tableload_start_time = str(pendulum.now(timezone))[:23]
    # read input table info and extract table/query details
    for table_info in table_infos:
        tableinfo_list = table_info.split("~")
        srctableid = tableinfo_list[0]
        srctablename = tableinfo_list[1]
        tgttablename = tableinfo_list[2]
        tgttablename = tgttablename.replace('v_parallon_ra_stage_dataset_name', bq_staging_dataset)
        tgttablenamecdc = tableinfo_list[3]
        tgttablenamecdc = tgttablenamecdc.replace('v_parallon_ra_stage_dataset_name', bq_staging_dataset)
        is_cdc = False if tgttablenamecdc == '' else True
        table_to_load_full = tgttablenamecdc if is_cdc else tgttablename
        table_to_load = table_to_load_full.split('.')[-1]
        tables_to_load_directly = ['mon_account_nonclinical_code_del', 'etl_cc_source_row_counts', 'mapcl_id', 'mapcl_svc_id']
        if table_to_load in tables_to_load_directly:
            continue
        preprocess_step = tableinfo_list[4]
        tgttableloadtype = tableinfo_list[5]
        temp_table_to_load_full = table_to_load_full + "_temp"

        primary_keys = tableinfo_list[6]
        if primary_keys != '' or primary_keys != 'N/A':
            primary_keys = [k.strip().lower() for k in primary_keys.split(',')]
        is_calc_job = True if tableinfo_list[7] == "Yes" else False
        srctablequery = tableinfo_list[8]
        srctablequery = srctablequery.replace('v_curr_timestamp', tableload_start_time)
        srctablequery = srctablequery.replace('$SchemaID', src_schema_id)
        # Replace all whitespace with a single space
        srctablequery = ' '.join(srctablequery.split())

        srctablequeryfull = tableinfo_list[9]
        srctablequeryfull = srctablequeryfull.replace('v_curr_timestamp', tableload_start_time)
        srctablequeryfull = srctablequeryfull.replace('$SchemaID', src_schema_id)
        # Replace all whitespace with a single space
        srctablequeryfull = ' '.join(srctablequeryfull.split())
        edw_etl_load_pre_update = True if tableinfo_list[10] == 'Yes' else False
        edw_etl_load_post_update = True if tableinfo_list[11] == 'Yes' else False

        if is_calc_job and 'all_calc_tables' in tables_to_ignore:
            logging.info("This is a calc job and a weekend run so a full pull will be done in a separate Dataflow job. Continuing to the next table")
            continue
        else:
            logging.info("This is a calc job but a weekday run so the normal query will be used")
        
        if is_cdc and table_to_load in tables_to_ignore:
            logging.info("This is a cdc job, but a full pull is being performed in a separate Dataflow job. Continuing to the next table")
            continue
        
        # In source queries, HCA_REPORTING.EDW_ETL_LOAD table is queried. We are now tracking that in BQ.
        # The following code will replace the subquery that selects from HCA_REPORTING with the result from querying in BQ.
        srctablequery = replace_edw_etl_load_field(srctablequery, table_to_load, src_schema_id)
        srctablequeryfull = replace_edw_etl_load_field(srctablequeryfull, table_to_load, src_schema_id)
        
        logging.info("Done replacing EDW ETL fields")

        if preprocess_step.lower() == 'delete':
            if table_to_load == 'mon_account_payer_calc_latest':
                purge_calc_latest_data(bq_client, 'mon_account_payer_calc_latest', 'mon_account_payer_calc_old', 'mapcl_id', 'id', 'id', src_schema_id)
            elif table_to_load == 'mon_account_payer_calc_service':
                purge_calc_data(bq_client, 'mon_account_payer_calc_service', 'mon_account_payer_calc_svc_old', 'mon_account_payer_calc_latest', 'mon_acct_payer_calc_summary_id', 'id', src_schema_id)
            elif is_cdc:
                logging.info(
                    "===Deleting rows from CDC table {} with srcSchema ={}===".format(tgttablenamecdc, src_schema_id))
                bq_client.query("DELETE FROM {} WHERE Schema_Id = {};".format(tgttablenamecdc, src_schema_id)).result()
                logging.info(
                    "===Inserting rows from table {} into CDC table {} with srcSchema ={}===".format(tgttablename, tgttablenamecdc, src_schema_id))
                bq_client.query("INSERT INTO {} SELECT * FROM {} WHERE Schema_Id = {};".format(tgttablenamecdc, tgttablename, src_schema_id)).result()
            else:
                logging.info(
                    "===Deleting rows from table {} with srcSchema ={}===".format(tgttablename, src_schema_id))
                bq_client.query("DELETE FROM {} WHERE Schema_Id = {};".format(tgttablename, src_schema_id)).result()
        
        elif preprocess_step.lower() == 'truncate':
            bq_client.query("truncate table " + table_to_load_full).result()
            logging.info(
                "===Truncated table {}===".format(table_to_load_full))

        # Delete existing records in the target table based on a column name
        if len(tableinfo_list) > 15:
            delete_based_on_column = tableinfo_list[15]
            if delete_based_on_column:
                logging.info(
                        "===Deleting rows from cdc table {} for column value {} with srcSchema ={}===".format(tgttablenamecdc, delete_based_on_column, src_schema_id))
                bq_client.query("DELETE FROM {} WHERE Schema_Id = {} and {} in (select {} from {} WHERE Schema_Id = {} );".format(tgttablenamecdc, src_schema_id, delete_based_on_column, delete_based_on_column, temp_table_to_load_full, src_schema_id)).result()

        if tgttableloadtype.lower() == 'append':
            logging.info("===Beginning insert from temp table {} to staging table {}===".format(
                temp_table_to_load_full, table_to_load_full))
            insert_str = f'INSERT INTO {table_to_load_full} SELECT * FROM {temp_table_to_load_full} WHERE schema_id = {src_schema_id}'
            logging.info(insert_str)
            bq_client.query(insert_str).result()
            logging.info("===Completed insert from temp table {} to staging table {}===".format(
                temp_table_to_load_full, table_to_load_full))
        if tgttableloadtype.lower() == 'merge':
            logging.info("===Beginning merge from temp table {} to staging table {}===".format(
                temp_table_to_load_full, table_to_load_full))
            merge_str = construct_merge_statement(bq_client, primary_keys, table_to_load_full, temp_table_to_load_full, src_schema_id)
            logging.info(merge_str)
            bq_client.query(merge_str).result()
            logging.info("===Completed merge from temp table {} to staging table {}===".format(
                temp_table_to_load_full, table_to_load_full))
        logging.info("===Deleting from temp table {} with src schema id {}===".format(temp_table_to_load_full, src_schema_id))
        bq_client.query("DELETE FROM {} WHERE Schema_Id = {};".format(temp_table_to_load_full, src_schema_id)).result()
        logging.info("===Deleted from temp table {} with src schema id {}===".format(temp_table_to_load_full, src_schema_id))
        
        post_processing_deletes(bq_client, table_to_load, src_schema_id)

        # Post process step to update edw_etl_load table
        if edw_etl_load_post_update:
            oracle_postprocess_update(bq_client, table_to_load, src_schema_id)

        # CDC Post Process step
        if is_cdc:
            logging.info(
                "===Deleting rows from table {} with srcSchema ={}===".format(tgttablename, src_schema_id))
            bq_client.query("DELETE FROM {} WHERE Schema_Id = {};".format(tgttablename, src_schema_id)).result()
            logging.info(
                "===Inserting rows from CDC table {} into table {} with srcSchema ={}===".format(tgttablenamecdc, tgttablename, src_schema_id))
            bq_client.query("INSERT INTO {} SELECT * FROM {} WHERE Schema_Id = {};".format(tgttablename, tgttablenamecdc, src_schema_id)).result()
            logging.info(
                "===Deleting rows from CDC table {} with srcSchema ={}===".format(tgttablenamecdc, src_schema_id))
            bq_client.query("DELETE FROM {} WHERE Schema_Id = {};".format(tgttablenamecdc, src_schema_id)).result()

def gcs_to_bq_load(table_key, schema_id):
    bq_client = bigquery.Client(bq_project_id)
    gcs_client = storage.Client()
    full_pull_config_file_name = 'ra_oracle_ingest_dependency_daily_gcs.yaml'
    full_pull_config = call_config_yaml(full_pull_config_file_name, 'ra_oracle_ingest_dependency_daily_gcs')

    if schema_id == 1:
        schema_instance = 'p1'
    elif schema_id == 3:
        schema_instance = 'p2'

    bucket_name = gcs_bucket
    folder_prefix = 'temp/edwradata/'
    gcs_temp_file_location = f'gs://{bucket_name}/{folder_prefix}'

    for table_config in full_pull_config[table_key]['table_info']:
        # Some Calc Full pull jobs are still a merge instead of truncate/load so we will use a temp table
        table_id = table_config['table_id_no_cdc']
        dataset_id = table_config['dataset_id']
        valid_table_keys=['all_calc_tables','mon_account_payer_calc_service_cdc_gg']
        if table_key in valid_table_keys:
            table_id_full = f'{bq_project_id}.{dataset_id}.{table_id}' + '_temp'
        else:
            table_id_full = f'{bq_project_id}.{dataset_id}.{table_id}'
        table_ref = bq_client.dataset(dataset_id).table(table_id)
        table = bq_client.get_table(table_ref)
        table_schema = table.schema
        table_schema_ordered = []

        source_schema = table_config['source_schema']
        for col_name in source_schema:
            matching_cols = [col for col in table_schema if col.name == str(col_name).lower()]
            # Check if a match is found before trying to append
            if matching_cols:
                table_schema_ordered.append(matching_cols[0])
            else:
                # Handle the case when no matching column is found
                print(f"Warning: Column '{col_name}' not found in table schema.")
                # Optionally, raise an error if the column is mandatory
                # raise ValueError(f"Column '{col_name}' not found in table schema.")
            #able_schema_ordered.append([col for col in table_schema if col.name == str(col_name).lower()][0])
        job_config = bigquery.job.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.field_delimiter = '|'
        job_config.schema = table_schema_ordered
        job_config._properties["load"]["preserve_ascii_control_characters"] = True
        # job_config.preserve_ascii_control_characters = True
        job_config.skip_leading_rows = 1

        # Delete from target BQ table
        delete_query = f"DELETE FROM {table_id_full} WHERE schema_id = {schema_id}"
        logging.info(f"Running query {delete_query}")
        bq_client.query(delete_query).result()
        
        # Load GCS shard files to BQ table
        uri = f'{gcs_temp_file_location}{table_id}/{schema_instance}*'
        logging.info(f"Loading files from {uri} to target table {table_id_full}")
        load_job = bq_client.load_table_from_uri(uri, table_id_full, job_config=job_config)
        load_job.result()

        # Some Calc Full pull jobs are merge instead of truncate/load
        if table_key in valid_table_keys:
            primary_keys = table_config['primary_keys']
            has_preprocess_truncate = table_config['has_preprocess_truncate']
            table_id_full_no_temp = table_id_full.replace('_temp', '')
            if has_preprocess_truncate is True:
                delete_query = f"DELETE FROM {table_id_full_no_temp} WHERE schema_id = {schema_id}"
                logging.info(f"Running query {delete_query}")
                bq_client.query(delete_query).result()
            logging.info("===Beginning merge from temp table {} to staging table {}===".format(table_id_full, table_id_full_no_temp))
            merge_str = construct_merge_statement(bq_client, primary_keys, table_id_full_no_temp, table_id_full, schema_id)
            logging.info(merge_str)
            bq_client.query(merge_str).result()
            logging.info("===Completed merge from temp table {} to staging table {}===".format(table_id_full, table_id_full_no_temp))
           
            post_processing_deletes(bq_client, table_id, schema_id)

        # Delete blob shards after load job
        logging.info(f"Deleting files in bucket {bucket_name} with prefix {folder_prefix}{table_id}/{schema_instance}")
        blobs = gcs_client.list_blobs(bucket_or_name=bucket_name, prefix=f"{folder_prefix}{table_id}/{schema_instance}")
        for blob in blobs:
            logging.info(f"Deleting blob {blob.name}")
            blob.delete()

        # Update edw_etl_load status table
        oracle_postprocess_update(bq_client, table_id, schema_id)

def update_cdc_airflow_var(airflow_var_name):
    from google.cloud import bigquery
    bqclient = bigquery.Client(bq_project_id)
    
    # Config file that lists parameters for the full pull tables
    full_pull_config_file_name = 'ra_oracle_ingest_dependency_daily_gcs.yaml'
    full_pull_config = call_config_yaml(full_pull_config_file_name, 'ra_oracle_ingest_dependency_daily_gcs')

    # cdc_ind table defines which tables should have a full pull done for them.
    get_cdc_ind_query = f'SELECT CAST(schema_id AS INTEGER) schema_id, CAST(cdc_ind.cdc_ind as INTEGER) cdc_ind, table_name FROM {bq_staging_dataset}.cdc_ind'
    res = bqclient.query(get_cdc_ind_query).result()
    logging.info(f"Completed query {get_cdc_ind_query}")

    full_pull_tables = []
    for row in res:
        # Since there are some tables in cdc_ind table that aren't being used, we have to check if the tables have full pull configs defined for them
        if row['table_name'].lower() in full_pull_config.keys():
            table_info = {"table_id": row['table_name'], "schema_id": row['schema_id'], "is_full_pull": False if row['cdc_ind'] == 1 else True}
            full_pull_tables.append(table_info)
        else:
            logging.info(f"Table {row['table_name']} not found in full pull config. If there is supposed to be a full pull for this job, add it to config file {full_pull_config_file_name}")

    # Calc jobs are always run every Saturday. Since they are smaller, they are all included in one Dataflow job
    if get_day_of_week() == 6:
        logging.info("Weekend run. All calc jobs need full pull")
        table_info_p1 = {"table_id": "all_calc_tables", "schema_id": 1, "is_full_pull": True}
        table_info_p2 = {"table_id": "all_calc_tables", "schema_id": 3, "is_full_pull": True}
        full_pull_tables.append(table_info_p1)
        full_pull_tables.append(table_info_p2)
    else:
        logging.info("Not a weekend run. Calc tables will only grab new rows based on calc_latest table")
        table_info_p1 = {"table_id": "all_calc_tables", "schema_id": 1, "is_full_pull": False}
        table_info_p2 = {"table_id": "all_calc_tables", "schema_id": 3, "is_full_pull": False}
        full_pull_tables.append(table_info_p1)
        full_pull_tables.append(table_info_p2)

    # New set of CDC jobs to run daily without any condition
    logging.info("Running additional daily CDC jobs")
    table_info_daily_p1 = {"table_id": "mon_account_payer_calc_service_cdc_gg","schema_id": 1, "is_full_pull": True}
    table_info_daily_p2 = {"table_id": "mon_account_payer_calc_service_cdc_gg","schema_id": 3, "is_full_pull": True}
    full_pull_tables.append(table_info_daily_p1)
    full_pull_tables.append(table_info_daily_p2)
    # Update Airflow variable with most recent data
    Variable.set(airflow_var_name, full_pull_tables, serialize_json=True)

def update_adhoc_airflow_var(airflow_var_name):
    from google.cloud import bigquery
    bqclient = bigquery.Client(bq_project_id)
    
    # Config file that lists parameters for the adhoc pull tables
    #full_pull_config_file_name = 'ra_oracle_ingest_dependency_adhoc.yaml'
    adhoc_pull_config_file_name = 'ra_oracle_ingest_dependency_adhoc.yaml'
    # full_pull_config = call_config_yaml(full_pull_config_file_name, 'ra_oracle_ingest_dependency_daily_gcs')
    adhoc_pull_config = call_config_yaml(adhoc_pull_config_file_name, 'ra_oracle_ingest_dependency_adhoc')

    
    # cdc_ind table defines which tables should have a full pull done for them.
    get_adhoc_ind_query = f'SELECT CAST(schema_id AS INTEGER) schema_id, CAST(adhoc_ind.adhoc_ind as INTEGER) adhoc_ind, table_name FROM {bq_staging_dataset}.adhoc_ind'
    res = bqclient.query(get_adhoc_ind_query).result()
    logging.info(f"Completed query {get_adhoc_ind_query}")

    adhoc_pull_tables = []
    for row in res:
        # Since there are some tables in adhoc_ind table that aren't being used, we have to check if the tables have adhoc pull configs defined for them
        if row['table_name'].lower() in adhoc_pull_config.keys():
            table_info = {"table_id": row['table_name'], "schema_id": row['schema_id'], "is_adhoc_pull": False if row['adhoc_ind'] == 1 else True}
            adhoc_pull_tables.append(table_info)
        else:
            logging.info(f"Table {row['table_name']} not found in adhoc pull config. If there is supposed to be a adhoc pull for this job, add it to config file {adhoc_pull_config_file_name}")

    # adhoc_pull_tables = []

    # # Adhoc Job 
    # logging.info("Running adhoc jobs")
    # table_info_daily_p1 = {"table_id": "mon_account_calc_detail_adhoc","schema_id": 1, "is_adhoc_pull": True}
    # table_info_daily_p2 = {"table_id": "mon_account_calc_detail_adhoc","schema_id": 3, "is_adhoc_pull": True}
    # adhoc_pull_tables.append(table_info_daily_p1)
    # adhoc_pull_tables.append(table_info_daily_p2)
    # Update Airflow variable with most recent data
    Variable.set(airflow_var_name, adhoc_pull_tables, serialize_json=True)

def gcs_to_bq_load_adhoc(table_key, schema_id):
    bq_client = bigquery.Client(bq_project_id)
    gcs_client = storage.Client()
    adhoc_pull_config_file_name = 'ra_oracle_ingest_dependency_adhoc.yaml'
    adhoc_pull_config = call_config_yaml(adhoc_pull_config_file_name, 'ra_oracle_ingest_dependency_adhoc')

    if schema_id == 1:
        schema_instance = 'p1'
    elif schema_id == 3:
        schema_instance = 'p2'

    bucket_name = gcs_bucket
    folder_prefix = 'temp/edwradata/'
    gcs_temp_file_location = f'gs://{bucket_name}/{folder_prefix}'

    for table_config in adhoc_pull_config[table_key]['table_info']:
        # Some Calc Full pull jobs are still a merge instead of truncate/load so we will use a temp table
        table_id = table_config['table_id_no_cdc']
        dataset_id = table_config['dataset_id']
        adhoc_table_keys=['mon_account_calc_detail_adhoc','ra_claim_category_adhoc','ra_service_category_adhoc','mon_account_comment_adhoc','cers_rate_adhoc']
        if table_key in adhoc_table_keys:
            table_id_adhoc = f'{bq_project_id}.{dataset_id}.{table_id}' 
        else:
            table_id_adhoc = f'{bq_project_id}.{dataset_id}.{table_id}' + '_temp'
        table_ref = bq_client.dataset(dataset_id).table(table_id)
        table = bq_client.get_table(table_ref)
        table_schema = table.schema
        table_schema_ordered = []

        source_schema = table_config['source_schema']
        for col_name in source_schema:
            matching_cols = [col for col in table_schema if col.name == str(col_name).lower()]
            # Check if a match is found before trying to append
            if matching_cols:
                table_schema_ordered.append(matching_cols[0])
            else:
                # Handle the case when no matching column is found
                print(f"Warning: Column '{col_name}' not found in table schema.")
                
        job_config = bigquery.job.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.field_delimiter = '|'
        job_config.schema = table_schema_ordered
        job_config._properties["load"]["preserve_ascii_control_characters"] = True
        # job_config.preserve_ascii_control_characters = True
        job_config.skip_leading_rows = 1

        # Delete from target BQ table
        delete_query = f"DELETE FROM {table_id_adhoc} WHERE schema_id = {schema_id}"
        logging.info(f"Running query {delete_query}")
        bq_client.query(delete_query).result()
        
        # Load GCS shard files to BQ table
        uri = f'{gcs_temp_file_location}{table_id}/{schema_instance}*'
        logging.info(f"Loading files from {uri} to target table {table_id_adhoc}")
        load_job = bq_client.load_table_from_uri(uri, table_id_adhoc, job_config=job_config)
        load_job.result()

        #post_processing_deletes(bq_client, table_id, schema_id)

        # Delete blob shards after load job
        logging.info(f"Deleting files in bucket {bucket_name} with prefix {folder_prefix}{table_id}/{schema_instance}")
        blobs = gcs_client.list_blobs(bucket_or_name=bucket_name, prefix=f"{folder_prefix}{table_id}/{schema_instance}")
        for blob in blobs:
            logging.info(f"Deleting blob {blob.name}")
            blob.delete()
            
        # Update edw_etl_load status table
        oracle_postprocess_update(bq_client, table_id, schema_id)