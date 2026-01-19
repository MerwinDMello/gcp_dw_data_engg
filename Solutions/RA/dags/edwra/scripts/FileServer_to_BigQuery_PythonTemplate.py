import os
import sys
import glob
import shutil
import tempfile
import logging
import traceback
import time
from datetime import datetime as dt
from datetime import timedelta
import pendulum
import argparse
import yaml
import json
import csv
import gzip
import string
import pandas as pd
import numpy as np
import math
import decimal
import random
# import ast
import re
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import secretmanager
from google.cloud.storage.retry import DEFAULT_RETRY
from google.api_core import exceptions
import apache_beam as beam
import paramiko
from paramiko import SSHClient
from ftplib import FTP
from jinja2 import Template
# from smb.SMBConnection import SMBConnection
from smbclient import open_file, register_session, delete_session, listdir, remove

cwd = os.path.dirname(os.path.abspath(__file__))
airflow_base_dir = os.path.dirname(cwd)
sys.path.insert(0, airflow_base_dir)
utilpath = airflow_base_dir + '/utils/'
config_folder = airflow_base_dir + "/config/"
sql_outbound_folder = airflow_base_dir + "/sql/dml/outbound/"

lob = "edwra"
lob = lob.lower().strip()
lob_abbr = lob.replace("edw","")

def call_config_yaml(config_folder, filename):
    # load input config .yaml file from config folder
    cfgfile = open(config_folder + filename)
    default_config = yaml.safe_load(cfgfile)
    default_config = json.loads(json.dumps(default_config, default=str))
    config = default_config
    return config

config = call_config_yaml(config_folder, f"{lob_abbr}_config.yaml")

current_timezone = pendulum.timezone("US/Central")

# Added for Retrying GCP APIs that fail due to timeout or failed health checks 
# Customize retry with a deadline of 180 seconds (default=120 seconds).
modified_retry = DEFAULT_RETRY.with_deadline(180)
# Customize retry with an initial wait time of 1.5 (default=1.0).
# Customize retry with a wait time multiplier per iteration of 1.2 (default=2.0).
# Customize retry with a maximum wait time of 15.0 (default=60.0).
modified_retry = modified_retry.with_delay(initial=1.5, multiplier=1.2, maximum=15.0)

def access_secret(secret_resourceid):
    logging.info("Get Secret Version {}".format(secret_resourceid))
    client = secretmanager.SecretManagerServiceClient()
    payload = client.access_secret_version(name=secret_resourceid).payload.data.decode("UTF-8")
    return payload

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

def get_blobs_list_using_prefix(bq_project_id, gcs_bucket, objectfullpath):
    # Get List of Objects based on Object Prefix
    try:
        logging.info("==== In get_blobs_list_using_prefix ====")
        logging.info("Project : {}, GCS Bucket : {}, Object Path : {}".format(
        bq_project_id, gcs_bucket, objectfullpath))
        
        bucket = get_bucket(bq_project_id, gcs_bucket)
        storage_client = storage.Client(bq_project_id)
        logging.info("Object Path {}".format(objectfullpath))
        blobs = storage_client.list_blobs(bucket, prefix=objectfullpath, delimiter=None, retry=modified_retry)
        return blobs
    except:
        logging.info("No blob with path {} is not found".format(objectfullpath))
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

def upload_object_from_file(bq_project_id, gcs_bucket, objectfullpath, filepath, file_extension):
    # Load file from local runtime directory to gcs bucket
    logging.info("==== In upload_object_from_file ====")
    logging.info("Project : {}, GCS Bucket : {}, Object Path : {}, File Path : {}".format(
        bq_project_id, gcs_bucket, objectfullpath, filepath))

    bucket = get_bucket(bq_project_id, gcs_bucket)
    blob = get_blob(bucket, objectfullpath)

    logging.info("File Extension : {}".format(file_extension))

    match file_extension:
        case "csv":
            content_type = "text/csv"
        case "xlsx":
            content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

    logging.info("Content Type : {}".format(content_type))       

    blob.upload_from_filename(filepath, content_type=content_type, retry=modified_retry)
    logging.info("Object {} has been loaded using file {}".format(blob.name, filepath))
    return blob.name

def download_object_to_file(bq_project_id, gcs_bucket, objectfullpath, filepath):
    # Download gcs bucket object to local runtime directory
    logging.info("===== In download_object_to_file =====")
    logging.info("Project : {}, GCS Bucket : {}, Object Path : {}, File Path : {}".format(
        bq_project_id, gcs_bucket, objectfullpath, filepath))

    bucket = get_bucket(bq_project_id, gcs_bucket)
    blob_exists = check_object_exists(bq_project_id, gcs_bucket, objectfullpath)
    if not blob_exists:
        logging.error("File is not present in GCS")
        raise SystemExit()
    blob = get_blob(bucket, objectfullpath)
    blob.download_to_filename(filepath, retry=modified_retry)
    if os.path.getsize(filepath) == 0:
        logging.error("Empty File, exiting")
        raise SystemExit()
    
    filelocalname = os.path.abspath(filepath)
    logging.info("Downloaded file {}. ".format(filelocalname))
    return None

def count_records_in_csv(filepath, encoding_scheme):

    record_count = sum(1 for _ in enumerate(open(filepath, encoding=encoding_scheme)))

    return record_count

def upload_object_from_dataframe(bq_project_id, gcs_bucket, objectfullpath, df, delimiter, quote_char, encoding_scheme, quoting, escape_char, header):
    # Load file from dataframe to gcs bucket
    logging.info("==== In upload_object_from_dataframe ====")
    logging.info("Project : {}, GCS Bucket : {}, Object Path : {}".format(
        bq_project_id, gcs_bucket, objectfullpath))

    bucket = get_bucket(bq_project_id, gcs_bucket)
    blob = get_blob(bucket, objectfullpath)
    logging.info("Object {} has been loaded from dataframe".format(blob.name))
    blob.upload_from_string(df.to_csv(sep=delimiter, quotechar=quote_char, header=header,
        encoding=encoding_scheme, quoting=quoting, escapechar=escape_char, index=False), retry=modified_retry)

    return blob.name

def archive_gcs_file(bq_project_id, gcs_bucket, objectfullpath, localfilepath, target_file_path, archive_method, file_extension):
    # Copy Source File in Archived folder with GZipped format
    logging.info("========== In archive_gcs_file ============")
    logging.info("Project : {}, GCS Bucket : {}, Object File Path : {}, File Path : {}, Target File Path : {}".format(bq_project_id, gcs_bucket, objectfullpath, localfilepath, target_file_path))
    bucket = get_bucket(bq_project_id, gcs_bucket)
    logging.info("Local Path {} : ".format(localfilepath))
    logging.info("Target Path {} : ".format(target_file_path))

    blob = get_blob(bucket, objectfullpath)
    if archive_method.lower() == "gzip":
        with open(localfilepath, 'rb') as src, gzip.open(target_file_path, 'wb') as dst:
            dst.writelines(src)

        blob.upload_from_filename(target_file_path, content_type='application/gzip', retry=modified_retry)
    else:
        with open(localfilepath, 'rb') as src, open(target_file_path, 'wb') as dst:
            shutil.copyfileobj(src, dst)

        match file_extension:
            case "csv":
                content_type = "text/csv"
            case "xlsx":
                content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

        blob.upload_from_filename(target_file_path, content_type=content_type, retry=modified_retry)
    logging.info("Source file archived at {}".format(blob.name))
    return blob.name

def convert_file(bq_project_id, gcs_bucket, objectfullpath, localfilepath, target_file_path, file_extension, conversion_type, input_file_instance_dict, output_file_instance_dict):
    import xlsxwriter
    # Convert Source File to Target File based on conversion method
    logging.info("========== In convert_file ============")
    logging.info("Project : {}, GCS Bucket : {}, Object File Path : {}, File Path : {}, Target File Path : {}".format(bq_project_id, gcs_bucket, objectfullpath, localfilepath, target_file_path))
    
    logging.info("Local Path {} : ".format(localfilepath))
    logging.info("Target Path {} : ".format(target_file_path))

    match conversion_type:
        case "excel_to_csv":
            bucket = get_bucket(bq_project_id, gcs_bucket)
            blob = get_blob(bucket, objectfullpath)
            delimiter = output_file_instance_dict['delimiter']
            quote_char = output_file_instance_dict['quote_char']
            encoding_scheme = output_file_instance_dict['encoding_scheme']
            quoting = output_file_instance_dict['quoting']
            escape_char = output_file_instance_dict['escape_char']
            header_record = output_file_instance_dict['header_record']
            if header_record == 'Yes':
                header = True
            else:
                header = False
            sheet_names = input_file_instance_dict['sheet_names']
            for sheet_name in sheet_names:
                read_file = pd.read_excel(localfilepath, sheet_name=sheet_name)
                read_file.to_csv(target_file_path, index = None, mode='a', header=header, sep=delimiter, quotechar=quote_char, encoding=encoding_scheme, quoting=quoting, escapechar=escape_char)

            match file_extension:
                case "csv":
                    content_type = "text/csv"

            blob.upload_from_filename(target_file_path, content_type=content_type, retry=modified_retry)
            logging.info("Output file created at {}".format(blob.name))
            return blob.name
        case "csv_to_excel":
            file_instance_dict = {}
            file_instance_dict['file_name_only'] = input_file_instance_dict['file_name_only']
            file_instance_dict['file_prefix'] = input_file_instance_dict['file_prefix']
            sheet_name = qualify_file_pattern(output_file_instance_dict['output_worksheet_pattern'], file_instance_dict)
            writer = output_file_instance_dict['writer']

            read_file = pd.read_csv(localfilepath)
            read_file.to_excel(writer, sheet_name=sheet_name, index=False)
            
            (max_row, max_col) = read_file.shape
            workbook = writer.book
            border_fmt = workbook.add_format({'bottom':2, 'top':2, 'left':2, 'right':2, 'border_color': '#0078D4'})
            worksheet = writer.sheets[sheet_name]
            worksheet.conditional_format(xlsxwriter.utility.xl_range(0, 0, max_row, max_col - 1), {'type': 'no_errors', 'format': border_fmt})
            worksheet.autofit()
            return None

def removegcsfile(bq_project_id, gcs_bucket, objectfullpath):
    logging.info("===== In removegcsfile params: =====")
    logging.info("Project : {}, GCS Bucket : {}, Object File Path : {}".format(
        bq_project_id, gcs_bucket, objectfullpath))
    bucket = get_bucket(bq_project_id, gcs_bucket)
    blob = bucket.blob(objectfullpath)
    if blob.exists():
        blob.delete(retry=modified_retry)
        logging.info("Source file {}  in GCS removed".format(blob.name))
    else:
        logging.info("Source file {}  in not found".format(blob.name))
    return blob.name

def removelocalfile(filenamestring):
    logging.info("==== In removelocalfile ====")
    logging.info("File name String : {}".format(filenamestring))
    filelist = glob.glob(filenamestring + '*')
    for file in filelist:
        os.remove(file)
    logging.info("Local file cleanup done")
    return None

def get_bq_params(bq_project_id):
    params = {
        f"param_parallon_{lob_abbr}_stage_dataset_name": f"{bq_project_id}.edw{lob_abbr}_staging",
        f"param_parallon_{lob_abbr}_core_dataset_name": f"{bq_project_id}.edw{lob_abbr}",
        f"param_parallon_{lob_abbr}_base_views_dataset_name": f"{bq_project_id}.edw{lob_abbr}_base_views",
        f"param_parallon_{lob_abbr}_views_dataset_name": f"{bq_project_id}.edw{lob_abbr}_views",
        f"param_parallon_{lob_abbr}_audit_dataset_name": f"{bq_project_id}.edw{lob_abbr}_ac"
        }
    return params

def get_resolved_table_name(bq_project_id, table_name):
    for dataset_type in ["stage","core"]:
        table_name = table_name.replace(
            f'v_parallon_{lob_abbr}_{dataset_type}_dataset_name', bq_project_id + "." + config['env'][f'v_parallon_{lob_abbr}_{dataset_type}_dataset_name'])
    logging.info("Table Name : {}".format(table_name))
    return table_name

def load_file_to_bigquery(bq_project_id, file_path, encoding_scheme, table_name, file_extension, delimiter, quote_char):
    bigquery_client = bigquery.Client(bq_project_id)

    write_disposition = 'WRITE_APPEND'
    skip_leading_rows = 0
    allow_jagged_rows = True

    match file_extension:
        case "csv":
            source_format = bigquery.SourceFormat.CSV

    job_config = bigquery.LoadJobConfig(
                write_disposition=write_disposition,
                field_delimiter=delimiter,
                autodetect=False,
                source_format=source_format,
                quote_character=quote_char,
                skip_leading_rows=skip_leading_rows,
                allow_jagged_rows=allow_jagged_rows,
                allow_quoted_newlines = True
            )

    source_file = open(file_path, 'rb')
    
    # Loading data from GCS to BigQuery Stage Tables 
    load_job = bigquery_client.load_table_from_file(
        file_obj=source_file,
        destination=table_name,
        job_config=job_config,
        num_retries=20
    )

    job_output = bigquery_client.get_job(load_job.job_id)

    # Waits for job to complete 
    while job_output.state != "DONE":
        time.sleep(5)
        job_output = bigquery_client.get_job(load_job.job_id)

    if job_output.state == "DONE":
        try:
            load_job.result()
            logging.info(f"==== Loading of file {file_path} from GCS to BQ has completed ====")
            logging.info(f"==== {job_output.output_rows} records have been loaded in the table {table_name} ====")
        except Exception as err:
            logging.error(f"==== Loading of file {file_path} from GCS to BQ has failed with error message - {err} ====")
            logging.error(traceback.format_exc())
            raise SystemExit()
    return job_output.output_rows

def execute_bq_query_from_template(bq_project_id, template, sql_dict, statement_type):
    sql_script = render_template(template, sql_dict)
    logging.info("SQL Script : {}".format(sql_script))
    if statement_type.lower() == "select":
        df = pd.read_gbq(sql_script, bq_project_id)
        return df
    else:
        for attempt in range(1, 20):
            try:
                bigquery_client = bigquery.Client(bq_project_id)
                bigquery_client.query(sql_script).result()
                break
                # pd.read_gbq(sql_script, bq_project_id, max_results=0)
            except exceptions.BadRequest as exc:
                # examine exc for details and handle the BadRequest error accordingly
                logging.info("data load attempt {} failed with exception {}".format(attempt, str(exc)))
                if ('Too many DML statements outstanding against table'.lower() in str(exc).lower() or 
                    'Could not serialize access to table'.lower() in str(exc).lower()):
                    # wait_time = (math.ceil(2.5 ** attempt))
                    wait_time = random.randint(10,60)
                    time.sleep(wait_time)
                else:
                    logging.info("Unhandled Exception : {}".format(exc))
                    raise SystemExit()
            except exceptions.Forbidden as exc:
                # examine exc for details and handle the Forbidden error accordingly
                logging.info("data load attempt {} failed with exception {}".format(attempt, str(exc)))
                if ('Your table exceeded quota for total number of dml jobs writing to a table'.lower() in str(exc).lower()):
                    wait_time = random.randint(10,60)
                    time.sleep(wait_time)
                else:
                    logging.info("Unhandled Exception : {}".format(exc))
                    raise SystemExit()
            except exceptions.InternalServerError as exc:
                # examine exc for details and handle the InternalServerError error accordingly
                logging.info("data load attempt {} failed with exception {}".format(attempt, str(exc)))
                if ('An internal error occurred and the request could not be completed'.lower() in str(exc).lower()):
                    wait_time = random.randint(10,60)
                    time.sleep(wait_time)
                else:
                    logging.info("Unhandled Exception : {}".format(exc))
                    raise SystemExit()
            except exceptions.ServiceUnavailable as exc:
                # examine exc for details and handle the ServiceUnavailable error accordingly
                logging.info("data load attempt {} failed with exception {}".format(attempt, str(exc)))
                wait_time = random.randint(10,60)
                time.sleep(wait_time)
            except Exception:
                # handle all other exceptions
                raise SystemExit()
        return None

def execute_post_processing_queries(bq_project_id, sql_options, sql_dict):
    try:
        table_name = sql_options["table_name"]
        if table_name.lower() == "staging":
            table_name = sql_dict["table_name"]
        sql_template_file_name = sql_options["sql_template_file_name"]
        template = sql_options["template"]
        execute_bq_query_from_template(bq_project_id, template, sql_dict, "Insert_Update_Delete")
        logging.info(f"==== Query execution for {table_name} using template {sql_template_file_name} has completed ====")
        return None
    
    except:
        logging.error(f"==== Query execution for {table_name} using template {sql_template_file_name} has failed ====")
        raise SystemExit()
        
def get_fileserver_credentials():
    protocol_type = fileserver_config_item['protocol_type']
    host = fileserver_config_item['file_server']
    if "port_no" in fileserver_config_item:
        port = fileserver_config_item['port_no']
    else:
        port = None
    user = fileserver_config_item['user_name']
    pwd_secret = config['env']['v_pwd_secrets_url'] + fileserver_config_item['pwd_secret_name']
    logging.info("Source System : {}, Pwd Secret : {}, Host : {}, User : {}, Port : {}".format(sourcesysnm, pwd_secret, host, user, port))
    password = access_secret(pwd_secret)
    return protocol_type, host, port, user, password

def get_share_mount_details():
    if "share_name" in fileserver_config_item:
        share_name = fileserver_config_item['share_name']
    else:
        share_name = None
    if "mount_drive" in fileserver_config_item:
        mount_drive = fileserver_config_item['mount_drive']
    else:
        mount_drive = None
    if "file_server_ip" in fileserver_config_item:
        file_server_ip = fileserver_config_item['file_server_ip']
    else:
        file_server_ip = None
    remote_directory = fileserver_config_item['directory']
    logging.info("Remote Directory : {}".format(remote_directory))
    return share_name, mount_drive, remote_directory, file_server_ip

def qualify_directory(directory_hierarchy, folder, source_system, process_group="", file_type="csv"):

    directory_path_words = directory_hierarchy.split("|")

    directory_path = directory_hierarchy.replace("|","/")

    if "SRC_FOLDER" in directory_path_words:
        directory_path = directory_path.replace("SRC_FOLDER", folder)

    if "SRCARCHIVE_FOLDER" in directory_path_words:
        directory_path = directory_path.replace("SRCARCHIVE_FOLDER", folder)

    if "SRC_SYSTEM" in directory_path_words:
        directory_path = directory_path.replace("SRC_SYSTEM", source_system)

    if "PROCESSING_GROUP" in directory_path_words:
        directory_path = directory_path.replace("PROCESSING_GROUP", process_group)

    if "FILE_TYPE" in directory_path_words:
        directory_path = directory_path.replace("FILE_TYPE", file_type)

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

def get_pattern_fmt_code(pattern, pattern_code):
    logging.info("Mask Required : {}".format(pattern['mask']))
            
    if str(pattern['mask']).upper() == 'YES':
        pattern_type = str(pattern['mask_info']['type']).lower()
        logging.info("Mask Type : {}".format(pattern_type))
        match pattern_type:
            case 'current_date':
                datetime_value = pendulum.now(current_timezone)
            case 'previous_weekday':
                datetime_value = pendulum.now(current_timezone)
                day_of_week = int(datetime_value.strftime('%w'))
                if day_of_week == 1:
                    date_diff = 3
                else:
                    date_diff = 1
                datetime_value = datetime_value - timedelta(days=date_diff)

        pattern_mask_code = str(pattern['mask_info']['code']).upper()
        logging.info("Mask Code : {}".format(pattern_mask_code))
        match pattern_mask_code:
            case 'YYYYMMDD':
                datetime_value_fmt = datetime_value.strftime("%Y%m%d")
            case 'DD-MMM-YYYY':
                datetime_value_fmt = datetime_value.strftime("%d-%b-%Y")
            case 'DD-MM-YYYY':
                datetime_value_fmt = datetime_value.strftime("%d-%m-%Y")

        pattern_fmt_code = pattern_code.replace(pattern_mask_code, datetime_value_fmt)
    else:
        pattern_fmt_code = pattern_code
    logging.info("Pattern Code : {}".format(pattern_fmt_code))
    return pattern_fmt_code

def get_file_pattern_details(pattern):
    pattern_code = pattern['input_file_pattern']
    logging.info("Pattern : {}".format(pattern_code))
    
    if "file_prefix" in pattern:
        pattern_prefix = pattern['file_prefix']
    else:
        pattern_prefix = None
    logging.info("Pattern Prefix : {}".format(pattern_prefix))
    
    if "source_extension" in pattern:
        source_extension = pattern['source_extension']
    else:
        source_extension = None
    
    if "archive_method" in pattern:
        archive_method = pattern['archive_method']
    else:
        archive_method = None

    pattern_fmt_code = get_pattern_fmt_code(pattern, pattern_code)

    return pattern_code, pattern_fmt_code, pattern_prefix, source_extension, archive_method

def get_target_table_name(pattern):
    target_table_name = pattern['target_table_name']
    return target_table_name

def get_terminal_file_pattern(pattern):
    if "terminal_file_pattern" in pattern:
        terminal_file_pattern = pattern['terminal_file_pattern']
    else:
        terminal_file_pattern = None
    return terminal_file_pattern

def get_sql_template_file_name(pattern):
    sql_template_file_name = pattern['sql_template_file_name']
    return sql_template_file_name

def get_column_names(pattern):
    column_names = pattern['column_names']
    return column_names

def get_output_file_pattern_details(pattern, file_extension, output_file_pattern):
    pattern_prefix = pattern['file_prefix']
    logging.info("Pattern Prefix : {}".format(pattern_prefix))

    pattern_fmt_code = get_pattern_fmt_code(pattern, output_file_pattern)
    file_instance_dict = {}
    file_instance_dict['file_extension'] = file_extension
    file_instance_dict['file_prefix'] = pattern_prefix
    output_file_name = qualify_file_pattern(pattern_fmt_code, file_instance_dict)

    sql_template_file_name = get_sql_template_file_name(pattern)
    column_names = get_column_names(pattern)

    return output_file_name, sql_template_file_name, column_names

def get_done_file_pattern(pattern):
    if "done_file" in pattern:
        done_file_pattern = pattern['done_file']
    else:
        done_file_pattern = None

    return done_file_pattern

def get_template_contents(template_file_name):
    file_location = os.path.join(df_base_dir, template_file_name)
    objectfullpath = '{}{}'.format(gcs_audit_notification_folder, template_file_name)
    logging.info("SQL Directory : {}".format(objectfullpath))
    download_object_to_file(bq_project_id, dag_gcs_bucket, objectfullpath, file_location)
    template_contents = read_contents_file(file_location)
    removelocalfile(file_location)
    return template_contents

def separate_sql_options(sql_list):
    if sql_list:
        for sql_item in sql_list:
            logging.info(sql_item["sql_template_file_name"])
            template_file_name = sql_item["sql_template_file_name"]
            template_contents = get_template_contents(template_file_name)
            sql_item.update({"template":template_contents})
        pre_sql_options_individual_files = list(filter(lambda d: d['execute_individual_file'] == True and d['processing_stage'].lower() == 'pre', sql_list))
        logging.info(pre_sql_options_individual_files)
        pre_sql_options_processes = list(filter(lambda d: d['execute_individual_file'] == False and d['processing_stage'].lower() == 'pre', sql_list))
        logging.info(pre_sql_options_processes)
        post_sql_options_individual_files = list(filter(lambda d: d['execute_individual_file'] == True and d['processing_stage'].lower() == 'post', sql_list))
        logging.info(pre_sql_options_individual_files)
        post_sql_options_processes = list(filter(lambda d: d['execute_individual_file'] == False and d['processing_stage'].lower() == 'post', sql_list))
        logging.info(pre_sql_options_processes)
    else:
        pre_sql_options_individual_files = None
        pre_sql_options_processes = None
        post_sql_options_individual_files = None
        post_sql_options_processes = None

    return pre_sql_options_individual_files, pre_sql_options_processes, post_sql_options_individual_files, post_sql_options_processes

def get_input_file_layout_patterns():

    input_file_layout = process_depend_config_item['input_file_layout']
    file_patterns = process_depend_config_item['file_patterns']
    file_name_mnemonic = process_depend_config_item['file_name_mnemonic']

    return input_file_layout, file_patterns, file_name_mnemonic

def get_output_file_layout_patterns():

    output_file_layout = process_depend_config_item['output_file_layout']
    output_file_pattern = process_depend_config_item['output_file_pattern']
    file_patterns = process_depend_config_item['file_patterns']

    return output_file_layout, output_file_pattern, file_patterns

def get_file_conversion_details():

    input_file_layout, file_patterns, file_name_mnemonic = get_input_file_layout_patterns()

    conversion_type = process_depend_config_item['conversion_type']

    output_file_layout = process_depend_config_item['output_file_layout']

    output_file_pattern = process_depend_config_item['output_file_pattern']

    return input_file_layout, file_name_mnemonic, conversion_type, output_file_layout, output_file_pattern, file_patterns

def get_excel_csv_conversion_details():

    sheet_names = process_depend_config_item['sheet_names']

    return sheet_names

def get_csv_excel_conversion_details():

    output_worksheet_pattern = process_depend_config_item['output_worksheet_pattern']

    return output_worksheet_pattern

def get_file_load_details():

    input_file_layout, file_patterns, file_name_mnemonic = get_input_file_layout_patterns()

    output_table_name = process_depend_config_item['output_table_name']

    if "id" in process_depend_config_item:
        file_id = process_depend_config_item['id']
    else:
        id = None
    
    if "preprocessing" in process_depend_config_item:
        preprocessing_combination = process_depend_config_item['preprocessing']
    else:
        preprocessing_combination = None
    
    if "missing_fields_position_combination_list" in process_depend_config_item:
        missing_fields_position_combination_list = process_depend_config_item['missing_fields_position_combination_list']
    else:
        missing_fields_position_combination_list = None
    
    if "reformat_field_positions" in process_depend_config_item:
        reformat_field_positions = process_depend_config_item['reformat_field_positions']
    else:
        reformat_field_positions = None
    
    if "remove_field_positions" in process_depend_config_item:
        remove_field_positions = process_depend_config_item['remove_field_positions']
    else:
        remove_field_positions = None

    if "tolerance_percent" in process_depend_config_item:
        tolerance_percent = process_depend_config_item['tolerance_percent']
    else:
        tolerance_percent = 0

    return file_id, input_file_layout, file_name_mnemonic, file_patterns, output_table_name, preprocessing_combination, missing_fields_position_combination_list, remove_field_positions, reformat_field_positions, tolerance_percent

def get_file_layout_details(file_layout):

    file_layout_item = next(filter(lambda item: item['layout'] == file_layout, process_config_item['file_layouts']),None)

    file_extension = file_layout_item['file_extension']
    original_directory_hierarchy = file_layout_item['original_directory_hierarchy']
    archive_directory_hierarchy = file_layout_item['archive_directory_hierarchy']
    
    if "add_date_suffix" in file_layout_item:
        add_date_suffix = file_layout_item['add_date_suffix']
    else:
        add_date_suffix = "No"
    
    if "delimiter" in file_layout_item:
        delimiter = file_layout_item['delimiter']
    else:
        delimiter = ","

    if "quote_char" in file_layout_item:
        quote_char = file_layout_item['quote_char']
    else:
        quote_char = '"'

    if "encoding_scheme" in file_layout_item:
        encoding_scheme = file_layout_item['encoding_scheme']
    else:
        encoding_scheme = "utf-8"

    if "header_record" in file_layout_item:
        header_record = file_layout_item['header_record']
    else:
        header_record = "No"

    if quote_char == "":
        quoting = csv.QUOTE_NONE
        escape_char = "\\"
    else:
        quoting = csv.QUOTE_MINIMAL
        escape_char = None
    
    return file_extension, original_directory_hierarchy, archive_directory_hierarchy, add_date_suffix, delimiter, quote_char, encoding_scheme, quoting, escape_char, header_record

def get_preprocessing_steps(preprocessing_combination):
    preprocessing_combination_record = next(filter(lambda item: item['combination'] == preprocessing_combination, process_config_item['preprocessing_combinations']),None)
    preprocessing_steps = preprocessing_combination_record['steps']
    return preprocessing_steps

def render_template(content_template, dict_instance):
    template_instance = Template(content_template)
    rendered_template = template_instance.render(dict_instance)
    return rendered_template

def read_contents_file(file_path, encoding_scheme='utf-8', read_lines=False):
    logging.info("==== In read_contents_file params: ====")
    logging.info("File Path : {}, Encoding Scheme : {}, Read Lines Flag : {}".format(
        file_path, encoding_scheme, read_lines))
    inputfile = open(file_path, 'r', encoding=encoding_scheme)
    try:
        if read_lines:
            return inputfile.readlines()
        else:
            return inputfile.read()
    except:
        logging.info("File couldn't be read in path {}".format(file_path))
        raise SystemExit()

''' 
remove_multiple_headers() function removes multiple headers from the source file

'''
def remove_multiple_headers(file_contents, line_terminator = "\n"):
    try:
        header_list = file_contents.split(line_terminator, 1)
        header = header_list[0]
        header_list = [rec for rec in header_list if rec != header]
        file_contents = (line_terminator).join(header_list)
        logging.info(
            "Content has been corrected to remove multiple headers")
        return file_contents
    except:
        logging.info(
            "Content couldn't be fixed to remove multiple headers")
        raise SystemExit()


''' 
remove_footer() function removes the footer record from the source file

'''
def remove_footer(file_contents):
    try:
        footer_list = file_contents.rsplit("\n", 1)
        if len(footer_list) == 1:
            footer = footer_list[0]
        else:
            footer = footer_list[1]
            if len(footer.strip()) == 0:
                footer_list = file_contents.rsplit("\n", 2)
                if len(footer_list) == 2:
                    footer = footer_list[0] + "\n"
                else:
                    footer = footer_list[1] + "\n"
        file_contents = file_contents.replace(footer,'')
        logging.info(
            "Content has been corrected to remove trailing summary footer")
        return file_contents
    except:
        logging.info(
            "Content couldn't be fixed to remove trailing summary footer")
        raise SystemExit()

''' 
suffix_field() function appends a column value to the source file 

'''
def suffix_field(file_contents, field_value):
    try:
        records = file_contents.split("\n")
        file_lines = []
        for index, line in enumerate(records):
            if line.strip() != '' :
                file_lines.append(f"{line},{field_value}")

        file_contents = ('\n').join(file_lines)
        logging.info(
            "Content has been appended")
        return file_contents
    except:
        logging.info(
            "Content couldn't be appended")
        raise SystemExit()

''' 
remove_records_for_missing_fields() function removes records that are empty for mandatory field combination 

'''
def remove_records_for_missing_fields(file_contents, delimiter, quote_char, missing_fields_position_combination_list):
    try:
        records = file_contents.split("\n")
        file_lines = []
        for index, line in enumerate(records):
            if line.strip() != '' :
                lines = []
                lines += [line.strip()]
                fields = []
                for row in csv.reader(lines, delimiter=delimiter, quotechar=quote_char):
                    for field in row:
                        if delimiter in field:
                            fields += [field.center(len(field) + 2, quote_char)]
                        else:
                            fields += [field]

                all_combinations_valid = True
                for missing_fields_position_combination in missing_fields_position_combination_list:                
                    missing_fields_positions = missing_fields_position_combination.split("|")
                    all_fields_in_combination_empty = True
                    for missing_fields_position in missing_fields_positions:
                        string_field = fields[int(missing_fields_position) - 1]
                        all_fields_in_combination_empty = True
                        if string_field is None or string_field.strip() == "":
                            continue
                        else:
                            all_fields_in_combination_empty = False
                    if all_fields_in_combination_empty == True:
                        all_combinations_valid = False

                if all_combinations_valid == True:
                    file_lines.append(line)

        file_contents = ('\n').join(file_lines)
        logging.info(
            "Records has been removed from the content")
        return file_contents
    except:
        logging.info(
            "Records couldn't be removed from the content")
        raise SystemExit()


''' 
remove_field() function removes the column based on field position in the source file 

'''

def remove_field(file_contents, field_position, delimiter, quote_char):
    try:
        records = file_contents.split("\n")
        file_lines = []
        for index, line in enumerate(records):
            if line.strip() != '' :
                lines = []
                lines += [line.strip()]
                fields = []
                for row in csv.reader(lines, delimiter=delimiter, quotechar=quote_char):
                    for field in row:
                        if delimiter in field:
                            fields += [field.center(len(field) + 2, quote_char)]
                        else:
                            fields += [field]
                fields.pop(field_position - 1)
                file_lines.append((delimiter).join(fields))

        file_contents = ('\n').join(file_lines)
        logging.info(
            "String Field has been padded")
        return file_contents
    except:
        logging.info(
            "String Field couldn't be padded", line)
        raise SystemExit()


''' 
reformat_time_field() function reformats the date or date_time field in the source file 

'''
def reformat_time_field(file_contents, field_position, delimiter, quote_char, source_date_format, target_date_format):
    # Convert Date value in Source Date / Date Time format to Target Date / Date Time format
    try:
        records = file_contents.split("\n")
        file_lines = []
        for index, line in enumerate(records):
            if line.strip() != '' :
                lines = []
                lines += [line.strip()]
                fields = []
                for row in csv.reader(lines, delimiter=delimiter, quotechar=quote_char):
                    for field in row:
                        if delimiter in field:
                            fields += [field.center(len(field) + 2, quote_char)]
                        else:
                            fields += [field]
                date_field = fields[field_position - 1]
                match source_date_format:
                    case "YYYY-MM-DD":
                        match target_date_format:
                            case "YYYY-MM-DD MM:HH:SS":
                                fields[field_position - 1] = dt.strptime(date_field,'%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S')
                    case "YYYYMMDD":
                        match target_date_format:
                            case "YYYY-MM-DD":
                                fields[field_position - 1] = dt.strptime(date_field,'%Y%m%d').strftime('%Y-%m-%d')
                    case "DD-MMM-YY":
                        match target_date_format:
                            case "YYYY-MM-DD":
                                fields[field_position - 1] = dt.strptime(date_field,'%d-%b-%y').strftime('%Y-%m-%d')
                    case "YYYY-MM-DD MM:HH:SS.FFF":
                        match target_date_format:
                            case "YYYY-MM-DD MM:HH:SS":
                                field_time_value = dt.strptime(date_field,'%Y-%m-%d %H:%M:%S.%f')
                                microseconds = field_time_value.microsecond
                                if microseconds > 499999:
                                    field_time_value = field_time_value.replace(microsecond=0) + timedelta(seconds=1)
                                new_date_field = field_time_value.strftime('%Y-%m-%d %H:%M:%S')
                                fields[field_position - 1] = new_date_field
                file_lines.append((delimiter).join(fields))

        file_contents = ('\n').join(file_lines)
        logging.info(
            "Date Field has been formatted")
        return file_contents
    except:
        logging.info(
            "Date Field couldn't be formatted", line)
        raise SystemExit()

''' 
reformat_field_remove_decimal() function removes the decimal values.

'''
def reformat_field_remove_decimal(file_contents, field_position, delimiter, quote_char):
    # Rounds the field to desired decimal positions
    try:
        records = file_contents.split("\n")
        file_lines = []
        for index, line in enumerate(records):
            if line.strip() != '' :
                lines = []
                lines += [line.strip()]
                fields = []
                for row in csv.reader(lines, delimiter=delimiter, quotechar=quote_char):
                    for field in row:
                        if delimiter in field:
                            fields += [field.center(len(field) + 2, quote_char)]
                        else:
                            fields += [field]
                string_field = fields[field_position - 1]
                if string_field.strip() == "":
                    fields[field_position - 1] = string_field.strip()
                else:
                    if "." in string_field:
                        fields[field_position - 1] = str(int(float(string_field)))
                    else:
                        fields[field_position - 1] = string_field.strip()
                file_lines.append((delimiter).join(fields))

        file_contents = ('\n').join(file_lines)
        logging.info(
            "Numeric Field has been truncated for decimal places")
        return file_contents
    except:
        logging.info(
            "Numeric Field couldn't be truncated for decimal places", line)
        raise SystemExit()

''' 
reformat_field_round_decimal_values() function rounds the field to desired decimal positions

'''
def reformat_field_round_decimal_values(file_contents, field_position, delimiter, quote_char, decimal_positions):
    # Rounds the field to desired decimal positions
    try:
        records = file_contents.split("\n")
        file_lines = []
        for index, line in enumerate(records):
            if line.strip() != '' :
                lines = []
                lines += [line.strip()]
                fields = []
                for row in csv.reader(lines, delimiter=delimiter, quotechar=quote_char):
                    for field in row:
                        if delimiter in field:
                            fields += [field.center(len(field) + 2, quote_char)]
                        else:
                            fields += [field]
                string_field = fields[field_position - 1]
                fields[field_position - 1] = str(round(float(string_field), decimal_positions))
                file_lines.append((delimiter).join(fields))

        file_contents = ('\n').join(file_lines)
        logging.info(
            "Numeric Field has been rounded")
        return file_contents
    except:
        logging.info(
            "Numeric Field couldn't be rounded", line)
        raise SystemExit()

''' 
reformat_field_replace_word() function replaces the string field

'''
def reformat_field_replace_word(file_contents, field_position, delimiter, quote_char, search_value, replace_value):
    # Replace Word in String Field
    try:
        match search_value:
            case "DOUBLE_QUOTE":
                search_value = '"'

        match replace_value:
            case "None":
                replace_value = " "
            case "NULL_VALUE":
                replace_value = ""
            case "SPACES_2":
                replace_value = "  "
            case "VALUE_YES":
                replace_value = "Yes"
            
        logging.info(f"Replace Value : {replace_value}")
        records = file_contents.split("\n")
        file_lines = []
        for index, line in enumerate(records):
            if line.strip() != '' :
                lines = []
                lines += [line.strip()]
                fields = []
                for row in csv.reader(lines, delimiter=delimiter, quotechar=quote_char):
                    for field in row:
                        if delimiter in field:
                            fields += [field.center(len(field) + 2, quote_char)]
                        else:
                            fields += [field]
                
                string_field = fields[field_position - 1]

                # Remove padded quotes from string field if search value and quote character is the same 
                if search_value == quote_char:
                    string_field = string_field.strip(quote_char)

                # Perform the replacement
                string_field = string_field.replace(search_value, replace_value)

                # Add back the quotes if search value and quote character is the same and delimiter is present
                if search_value == quote_char and delimiter in string_field:
                    string_field = string_field.center(len(string_field) + 2, quote_char)

                fields[field_position - 1] = string_field

                file_lines.append((delimiter).join(fields))

        file_contents = ('\n').join(file_lines)
        logging.info(
            "String Field has been replaced")
        return file_contents
    except:
        logging.info(
            "String Field couldn't be replaced", line)
        raise SystemExit()

''' 
reformat_field_substring_extract() function reformats the string field in the source file to pad characters as per the requested direction

'''
def reformat_field_substring_extract(file_contents, field_position, delimiter, quote_char, scan_direction, skip_characters, substring_length):
    # Pad Fill Characters to String Field to match desired field length
    try:
        records = file_contents.split("\n")
        file_lines = []
        for index, line in enumerate(records):
            if line.strip() != '' :
                lines = []
                lines += [line.strip()]
                fields = []
                for row in csv.reader(lines, delimiter=delimiter, quotechar=quote_char):
                    for field in row:
                        if delimiter in field:
                            fields += [field.center(len(field) + 2, quote_char)]
                        else:
                            fields += [field]
                string_field = fields[field_position - 1]
                if string_field is None or string_field == "" :
                    fields[field_position - 1] = ""
                else:
                    match scan_direction:
                        case "left":
                            fields[field_position - 1] = string_field[skip_characters: skip_characters + substring_length]
                        case "right":
                            if skip_characters == 0:
                                fields[field_position - 1] = string_field[-1 * substring_length: ]
                            else:
                                fields[field_position - 1] = string_field[-1 * (skip_characters + substring_length): -1 * skip_characters]
                # logging.info("Converted String {}".format(fields[field_position - 1]))
                file_lines.append((delimiter).join(fields))

        file_contents = ('\n').join(file_lines)
        logging.info(
            "String Field has been extracted for substring")
        return file_contents
    except:
        logging.info(
            "String Field couldn't be extracted for substring", line)
        raise SystemExit()

''' 
reformat_field_pad_chars() function reformats the string field in the source file to pad characters as per the requested direction

'''
def reformat_field_pad_chars(file_contents, field_position, delimiter, quote_char, pad_direction, field_length, fill_char):
    # Pad Fill Characters to String Field to match desired field length
    try:
        records = file_contents.split("\n")
        file_lines = []
        for index, line in enumerate(records):
            if line.strip() != '' :
                lines = []
                lines += [line.strip()]
                fields = []
                for row in csv.reader(lines, delimiter=delimiter, quotechar=quote_char):
                    for field in row:
                        if delimiter in field:
                            fields += [field.center(len(field) + 2, quote_char)]
                        else:
                            fields += [field]
                string_field = fields[field_position - 1]
                if string_field is None or string_field == "" :
                    fields[field_position - 1] = ""
                else:
                    match pad_direction:
                        case "left":
                            fields[field_position - 1] = string_field.rjust(field_length, fill_char)
                        case "right":
                            fields[field_position - 1] = string_field.ljust(field_length, fill_char)
                    if fields[field_position - 1] == (field_length * fill_char):
                        fields[field_position - 1] = ""
                file_lines.append((delimiter).join(fields))

        file_contents = ('\n').join(file_lines)
        logging.info(
            "String Field has been padded")
        return file_contents
    except:
        logging.info(
            "String Field couldn't be padded", line)
        raise SystemExit()

def preprocess_step_execution(preprocessing_steps, file_location, encoding_scheme, delimiter, quote_char, file_instance_dict):
    logging.info("Preprocessing Steps {}".format(preprocessing_steps))

    file_contents = read_contents_file(file_location, encoding_scheme)

    expected_count_with_bad_record_count = 0

    if preprocessing_steps:
        if "reformat_field_positions" in file_instance_dict:
            reformat_field_positions_iteration = iter(str(file_instance_dict['reformat_field_positions']).split("|"))
        if "remove_field_positions" in file_instance_dict:
            remove_field_positions_iteration = iter(str(file_instance_dict['remove_field_positions']).split("|"))
        for preprocess_step in preprocessing_steps:
            logging.info("Step {}".format(preprocess_step["name"]))
            match preprocess_step["name"]:
                case "remove_header":
                    file_contents = remove_multiple_headers(file_contents)
                case "remove_records_for_missing_fields":
                    expected_count_with_bad_record_count = len([l for l in file_contents.split("\n") if l.strip(' ') != ''])
                    missing_fields_position_combination_list = file_instance_dict['missing_fields_position_combination_list']
                    file_contents = remove_records_for_missing_fields(file_contents, delimiter, quote_char, missing_fields_position_combination_list)
                case "add_file_name":
                    file_contents = suffix_field(file_contents, file_instance_dict['file_name_only'])
                case "add_file_name_with_extension":
                    file_contents = suffix_field(file_contents, f"{file_instance_dict['file_name_only']}.{file_instance_dict['file_extension']}")
                case "add_file_name_with_source_extension":
                    file_contents = suffix_field(file_contents, f"{file_instance_dict['file_name_only']}.{file_instance_dict['source_extension']}")
                case "add_subdirectory":
                    file_contents = suffix_field(file_contents, file_instance_dict['process_group'])
                case "add_file_word":
                    search_field = preprocess_step["search_field"]
                    logging.info(search_field)
                    search_pattern = preprocess_step["search_pattern"]
                    if "FILE_NAME_PREFIX" in search_pattern:
                        search_pattern = search_pattern.replace("FILE_NAME_PREFIX", file_instance_dict['file_prefix'])
                    logging.info(search_pattern)
                    match search_field:
                        case "file_name_only":
                            search_field_value = file_instance_dict['file_name_only']
                    logging.info(search_field_value)
                    new_field = re.search(search_pattern, search_field_value, re.IGNORECASE)
                    logging.info(new_field)
                    file_contents = suffix_field(file_contents, new_field.group())
                case "remove_field":
                    field_position = int(next(remove_field_positions_iteration))
                    logging.info("Field Position {}".format(field_position))
                    file_contents = remove_field(file_contents, field_position, delimiter, quote_char)
                case "reformat_time_field":
                    field_position = int(next(reformat_field_positions_iteration))
                    logging.info("Field Position {}".format(field_position))
                    source_date_format = preprocess_step["source_time_format"]
                    target_date_format = preprocess_step["target_time_format"]
                    file_contents = reformat_time_field(file_contents, field_position, delimiter, quote_char, source_date_format, target_date_format)
                case "reformat_field_remove_decimal":
                    field_position = int(next(reformat_field_positions_iteration))
                    logging.info("Field Position {}".format(field_position))
                    file_contents = reformat_field_remove_decimal(file_contents, field_position, delimiter, quote_char)
                case "reformat_field_round_decimal_values":
                    field_position = int(next(reformat_field_positions_iteration))
                    logging.info("Field Position {}".format(field_position))
                    decimal_positions = int(preprocess_step["decimal_positions"])
                    logging.info(f"Decimal Positions : {decimal_positions}")
                    file_contents = reformat_field_round_decimal_values(file_contents, field_position, delimiter, quote_char, decimal_positions)
                case "reformat_field_replace_word":
                    field_position = int(next(reformat_field_positions_iteration))
                    logging.info("Field Position {}".format(field_position))
                    search_value = str(preprocess_step["search_value"])
                    logging.info(f"Search Value : {search_value}")
                    replace_value = str(preprocess_step["replace_value"])
                    logging.info(f"Replace Value : {replace_value}")
                    file_contents = reformat_field_replace_word(file_contents, field_position, delimiter, quote_char, search_value, replace_value)
                case "reformat_field_substring_extract":
                    field_position = int(next(reformat_field_positions_iteration))
                    logging.info("Field Position {}".format(field_position))
                    scan_direction = preprocess_step["scan_direction"]
                    skip_characters = int(preprocess_step["skip_characters"])
                    substring_length = int(preprocess_step["substring_length"])
                    file_contents = reformat_field_substring_extract(file_contents, field_position, delimiter, quote_char, scan_direction, skip_characters, substring_length)
                case "reformat_field_pad_chars":
                    field_position = int(next(reformat_field_positions_iteration))
                    logging.info("Field Position {}".format(field_position))
                    pad_direction = preprocess_step["pad_direction"]
                    field_length = int(preprocess_step["field_length"])
                    fill_char = str(preprocess_step["fill_char"])
                    file_contents = reformat_field_pad_chars(file_contents, field_position, delimiter, quote_char, pad_direction, field_length, fill_char)

    current_time = str(pendulum.now(current_timezone).strftime("%Y-%m-%d %H:%M:%S"))
    file_contents = suffix_field(file_contents, current_time)
    
    with open(file_location, 'w', encoding=encoding_scheme) as output:
        output.seek(0)
        output.truncate()
        output.write(file_contents)
    
    expected_count = count_records_in_csv(file_location, encoding_scheme)

    if expected_count_with_bad_record_count == 0:
        expected_count_with_bad_record_count = expected_count

    return file_location, expected_count_with_bad_record_count, expected_count

class file_processing(beam.DoFn):

    def set_sftp_connection(self, host, port, user, password):
        logging.info("==== In set_sftp_connection ====")
        ssh_client = paramiko.SSHClient()
        ssh_client.load_system_host_keys()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname=host, port=port, username=user, password=password, banner_timeout=3600)
        logging.info("connection successful and logged in")
        #open sftp
        sftp_client=ssh_client.open_sftp()
        logging.info("SFTP opened")
        return sftp_client

    def set_ftp_connection(self, host, port, user, password):
        logging.info("==== In set_ftp_connection ====")
        ftp_client = FTP()
        
        # Connect to Host with port
        ftp_client.connect(host=host, port=port, timeout=3600)
        
        # Login to the Host Server
        ftp_client.login(user=user, passwd=password)
        logging.info("connection successful and logged in")
        logging.info("FTP opened")
        return ftp_client
    
    def set_unix_mount(self, host, user, password, share_name):
        logging.info("Server {}".format(host))
        logging.info("User {}".format(user))
        logging.info("Share Name {}".format(share_name))
        os.system(f'apt update')
        os.system(f'apt install cifs-utils -y')
        os.system(f'mkdir -p /mnt/{share_name}')
        logging.info(os.system(f"mount -t cifs //host/{share_name} /mnt/{share_name} -o username='user',password='password'"))
        os.system(f'df -h')
        return f"/mnt/{share_name}"
    
    def set_windows_mount(self, host, user, password, share_name, mount_drive):
        logging.info("Server {}".format(host))
        logging.info("User {}".format(user))
        logging.info("Share Name {}".format(share_name))
        logging.info("Mount Drive {}".format(mount_drive))
        shared_drive = r"\\" + f"{host}\{share_name}"
        os.system(f'net use {mount_drive}: /del')
        os.system(f'net use {mount_drive}: {shared_drive}')
        return f"{mount_drive}:"
    
    def set_smb_connection(self, host, port, user, password, file_server_ip):
        logging.info("Server {}".format(host))
        logging.info("Server {}".format(port))
        logging.info("User {}".format(user))
        logging.info("File Server IP {}".format(file_server_ip))
        # pysmb
        # smb_client = SMBConnection(username=user, password=password, my_name="my_client", remote_name=host, use_ntlm_v2=True, is_direct_tcp=True)
        # smb_client.connect(ip=file_server_ip, port=port)

        #smbprotocol
        smb_client = register_session(f"{host}",username=user,password=password)
        return smb_client

    def create_fileserver_connection(self, protocol_type, host, port, user, password, share_name, mount_drive, file_server_ip):
        logging.info("==== In create_fileserver_connection ====")
        match protocol_type:
            case "sftp":
                self.fileserver_client = self.set_sftp_connection(host, port, user, password)
            case "ftp":
                self.fileserver_client = self.set_ftp_connection(host, port, user, password)
            case "smb":
                self.fileserver_client = self.set_smb_connection(host, port, user, password, file_server_ip)
            case "mount":
                if beam_runner == 'DataflowRunner':
                    self.mounted_drive = self.set_unix_mount(host, user, password, share_name)
                else:
                    self.mounted_drive = self.set_windows_mount(host, user, password, share_name, mount_drive)

    def list_directories(self, protocol_type, directory=".", host="", share_name="", terminal_file_pattern="*"):
        match protocol_type:
            case "sftp":
                return self.fileserver_client.listdir(directory), directory
            case "ftp":
                object_list = []
                self.fileserver_client.retrlines('NLST {}'.format(directory), object_list.append)
                return object_list, directory
            case "smb":
                # pysmb
                # smb_listdir = self.fileserver_client.listPath(share_name, directory)
                # return [file.filename for file in smb_listdir]

                #smbprotocol
                full_path = r"\\" + f"{host}\{share_name}\{directory}"
                return listdir(path=full_path, search_pattern=terminal_file_pattern), full_path
            case "mount":
                full_path = os.path.join(self.mounted_drive, directory)
                return os.listdir(full_path), full_path

    def download_remote_file(self, protocol_type, remote_location, file_location, host="", share_name = ""):
        logging.info("==== In download_remote_file ====")  
        match protocol_type:
            case "sftp":
                self.fileserver_client.get(remote_location, file_location)
                fileserver_file_instance = self.fileserver_client.open(remote_location, 'r')
                with open(file_location, 'wb') as fo:
                    shutil.copyfileobj(fileserver_file_instance, fo)
            case "ftp":
                with open(file_location, 'wb') as fo:
                    self.fileserver_client.retrbinary('RETR %s' % remote_location, fo.write)
            case "smb":
                # pysmb
                # with open(file_location, 'wb') as fo:
                    # self.fileserver_client.retrieveFile(f"{share_name}", f"{remote_location}", fo)

                #smbprotocol
                remote_location = r"\\" + f"{host}\{share_name}\{remote_location}"
                fileserver_file_instance = open_file(remote_location, mode="rb")
                with open(file_location, 'wb') as fo:
                    shutil.copyfileobj(fileserver_file_instance, fo)
                
            case "mount":
                fileserver_file_instance = open(os.path.join(self.mounted_drive, remote_location), 'rb')
                with open(file_location, 'wb') as fo:
                    shutil.copyfileobj(fileserver_file_instance, fo)
        return remote_location

    def upload_remote_file(self, protocol_type, remote_location, file_location, host="", share_name = ""):
        logging.info("==== In upload_remote_file ====")
        logging.info(file_location)
        logging.info(remote_location)

        match protocol_type:
            case "sftp":
                self.fileserver_client.put(file_location, remote_location)
            case "ftp":
                with open(file_location, 'rb') as fi:
                    self.fileserver_client.storbinary('STOR %s' % remote_location, fi)
            case "smb":
                # pysmb
                # with open(file_location, 'rb') as fi:
                    # self.fileserver_client.storeFile(f"{share_name}", f"{file_location}", fi)

                #smbprotocol
                local_file_instance = open(file_location, 'rb')
                with open_file(r"\\" + f"{host}\{share_name}\{remote_location}", mode="wb") as fd:
                    shutil.copyfileobj(local_file_instance, fo)
            case "mount":
                local_file_instance = open(file_location, 'rb')
                with open(os.path.join(self.mounted_drive, remote_location), 'wb') as fo:
                    shutil.copyfileobj(local_file_instance, fo)

    def delete_remote_file(self, protocol_type, remote_location, host="", share_name = ""):
        match protocol_type:
            case "sftp":
                self.fileserver_client.remove(remote_location)
            case "ftp":
                self.fileserver_client.delete(remote_location)
            case "smb":
                # pysmb
                # self.fileserver_client.deleteFiles(share_name, remote_location)

                #smbprotocol
                remove(r"\\" + f"{host}\{share_name}\{remote_location}")
            case "mount":
                os.remove(os.path.join(self.mounted_drive, remote_location))

    def close_fileserver_connection(self, protocol_type, host=""):
        match protocol_type:
            case "sftp":
                self.fileserver_client.close()
            case "ftp":
                self.fileserver_client.quit()
            case "smb":
                # pysmb
                # self.fileserver_client.close()

                #smbprotocol
                delete_session(f"{host}")
            case "mount":
                if beam_runner == 'DataflowRunner':
                    os.system(f'umount {self.mounted_drive}')
                else:
                    os.system(f'net use {self.mounted_drive} /del')

    def initialize_file_info(self):
        self.file_details = {}
        self.patterns_found = []

    def set_file_name_pattern(self, file_name_pattern):
        logging.info("File Pattern : {}".format(file_name_pattern))
        self.file_details[file_name_pattern] = {}

    def set_file_locations(self, file_name, remote_directory, df_base_dir):
        remote_location = os.path.join(remote_directory, file_name)
        logging.info("Remote Path {}".format(remote_location))
        file_location = self.set_local_file_location(file_name, df_base_dir)
        logging.info("Local Path {}".format(file_location))
        return remote_location, file_location

    def set_local_file_location(self, file_name, df_base_dir):
        file_location = os.path.join(df_base_dir, file_name)
        logging.info("Local Path {}".format(file_location))
        return file_location

    def set_file_name_line_count(self, file_name_pattern, file_name, file_location, pattern_code, file_extension):
        file_line_count=0
        if process_type.lower() in ["get_files", "put_files"]:
            if file_extension in ("csv", "txt"):
                with open(file_location, 'rb') as fi:
                    file_line_count = sum(1 for _ in fi)
        logging.info(f"File Name {file_location}, Record count {file_line_count}")
        self.file_details[file_name_pattern][file_name] = file_line_count
        if pattern_code not in self.patterns_found:
            self.patterns_found.append(pattern_code)

    def check_patterns_found(self, file_pattern):
        logging.info(f"below file patterns are found on fileserver server.... {self.patterns_found} while searching for {file_pattern}")
        if len(self.patterns_found) != len(file_pattern):
            # logging.info(f"few file patterns cannot be found on fileserver server. failing... {self.patterns_found}")
            # raise SystemExit()
            logging.warning(f"Some files not found to delete. Proceeding with rest of process... {self.patterns_found}")
            # raise SystemExit()

    def fileserver_get_delete_files(self):
        logging.info("==== In fileserver_get_delete_files ====")
        
        try:
            protocol_type, host, port, user, password = get_fileserver_credentials()
            share_name, mount_drive, remote_directory, file_server_ip = get_share_mount_details()
            self.create_fileserver_connection(protocol_type, host, port, user, password, share_name, mount_drive, file_server_ip)
            listing, full_path = self.list_directories(protocol_type=protocol_type, host=host, share_name=share_name)
            logging.info("Directory List : {}".format(listing))

            logging.info("SQL List : {}".format(sql_list))

            pre_sql_options_individual_files, pre_sql_options_processes, post_sql_options_individual_files, post_sql_options_processes = separate_sql_options(sql_list)
            logging.info("Pre SQL List - Every Record : {}".format(pre_sql_options_individual_files))
            logging.info("Pre SQL List - Entire Process : {}".format(pre_sql_options_processes))
            logging.info("Post SQL List - Every Record : {}".format(post_sql_options_individual_files))
            logging.info("Post SQL List - Entire Process : {}".format(post_sql_options_processes))

            input_file_layout, file_patterns, file_name_mnemonic = get_input_file_layout_patterns()
            logging.info("Input File Layout : {}".format(input_file_layout))
            logging.info("File Patterns : {}".format(file_patterns))
            logging.info("File Mnemonic : {}".format(file_name_mnemonic))

            file_extension, original_directory_hierarchy, archive_directory_hierarchy, add_date_suffix, delimiter, quote_char, encoding_scheme, quoting, escape_char, header_record = get_file_layout_details(input_file_layout)
            logging.info("File Extension : {}".format(file_extension))
            logging.info("Original Directory Hierarchy : {}".format(original_directory_hierarchy))
            logging.info("Archive Directory Hierarchy : {}".format(archive_directory_hierarchy))
            logging.info("Add Date Suffix : {}".format(add_date_suffix))

            original_directory_path = qualify_directory(original_directory_hierarchy, folder=src_folder, source_system=sourcesysnm, process_group=process_group, file_type=file_extension)
            logging.info("Original Directory Path : {}".format(original_directory_path))

            if process_group == None:
                remote_directory_path = remote_directory
            else:
                remote_directory_path = os.path.join(remote_directory, process_group)

            listing_remote_dir, full_path =  self.list_directories(protocol_type=protocol_type, directory=remote_directory_path, host=host, share_name=share_name)
            logging.info("Files in Directory : {}".format(listing_remote_dir))

            self.initialize_file_info()

            file_instance_dict = {}
            file_instance_dict['file_extension'] = file_extension
            file_instance_dict['process_group'] = process_group
            for pattern in file_patterns:
                pattern_code, pattern_fmt_code, pattern_prefix, source_extension, archive_method = get_file_pattern_details(pattern)
                logging.info(pattern_fmt_code)
                file_name_pattern = re.sub(r"[^a-zA-Z0-9_-]+", '', pattern_fmt_code.split('.')[0])
                self.set_file_name_pattern(file_name_pattern)

                target_table_name = get_target_table_name(pattern)

                if pre_sql_options_individual_files:
                    for pre_sql_options_individual_file in pre_sql_options_individual_files:
                        sql_dict = {'params': get_bq_params(bq_project_id)}
                        match pre_sql_options_individual_file['sql_template_file_name']:
                            case "delete_staging.j2":
                                sql_dict.update({"table_name": target_table_name, "file_name_mnemonic": pattern_prefix, 
                                                "operational_group": process_group})
                            case "delete_source_filenames.j2":
                                sql_dict.update({"file_name_mnemonic": pattern_prefix, "operational_group": process_group})
                            case "delete_target_filenames.j2":
                                sql_dict.update({"file_name_mnemonic": pattern_prefix, "operational_group": process_group})
                            case "delete_folder_audit.j2":
                                sql_dict.update({"file_name_mnemonic": pattern_prefix, "operational_group": process_group})
                            case "delete_excel_audit.j2":
                                sql_dict.update({"file_name_mnemonic": pattern_prefix, "operational_group": process_group})
                        execute_post_processing_queries(bq_project_id, pre_sql_options_individual_file, sql_dict)

                source_file_count = 0

                terminal_file_pattern = get_terminal_file_pattern(pattern)

                if terminal_file_pattern:
                    listing_remote_dir, full_path =  self.list_directories(protocol_type=protocol_type, directory=remote_directory_path, host=host, share_name=share_name, terminal_file_pattern=terminal_file_pattern)
                    logging.info("Files in Directory : {}".format(listing_remote_dir))

                for file_name in listing_remote_dir:
                    is_pattern = re.search(pattern_fmt_code, file_name, re.IGNORECASE)
                    if is_pattern:
                        logging.info("File matching regex {}".format(file_name))

                        source_file_count = source_file_count + 1

                        remote_location, file_location = self.set_file_locations(file_name, remote_directory_path, df_base_dir)
                        
                        match process_type.lower():
                            case "get_files": 
                                full_remote_location = self.download_remote_file(protocol_type, remote_location, file_location, host, share_name)
                                self.set_file_name_line_count(file_name_pattern, file_name, file_location, pattern_code, file_extension)

                                dest_objectfullpath = '{}{}'.format(original_directory_path, file_name)
                                upload_object_from_file(bq_project_id, stnd_gcs_bucket, dest_objectfullpath, file_location, file_extension)
                                removelocalfile(file_location)

                                if post_sql_options_individual_files:
                                    for post_sql_options_individual_file in post_sql_options_individual_files:
                                        sql_dict = {'params': get_bq_params(bq_project_id)}
                                        match post_sql_options_individual_file['sql_template_file_name']:
                                            case "insert_source_filenames.j2":
                                                sql_dict.update({"file_name_mnemonic": pattern_prefix, "operational_group": process_group,
                                                    "file_name": file_name, "file_path": full_remote_location.replace(chr(92), chr(92)+chr(92))})
                                        execute_post_processing_queries(bq_project_id, post_sql_options_individual_file, sql_dict)

                            case "delete_files": 
                                self.set_file_name_line_count(file_name_pattern, file_name, file_location, pattern_code, file_extension)
                                self.delete_remote_file(protocol_type, remote_location, host, share_name)

                if post_sql_options_processes:
                    for post_sql_options_process in post_sql_options_processes:
                        sql_dict = {'params': get_bq_params(bq_project_id)}
                        match post_sql_options_process['sql_template_file_name']:
                            case "insert_folder_audit.j2":
                                if source_file_count > 0:
                                    sql_dict.update({"source_folder_path": f"{full_path.replace(chr(92), chr(92)+chr(92))}",
                                                    "folder_file_count": source_file_count, "file_name_mnemonic": pattern_prefix,
                                                    "operational_group": process_group})
                                    execute_post_processing_queries(bq_project_id, post_sql_options_process, sql_dict)

            self.check_patterns_found(file_patterns)

        except:
            logging.error(
                "===ERROR: Failure occurred within fileserver_get_delete_files function===")
            logging.error(traceback.format_exc())
            raise SystemExit()
        
        self.close_fileserver_connection(protocol_type, host)
        logging.info("Connection Closed!")
        logging.info(self.file_details)
        return self.file_details

    def fileserver_put_files(self):
        logging.info("==== In fileserver_put_files ====")
        
        try:
            
            protocol_type, host, port, user, password = get_fileserver_credentials()
            share_name, mount_drive, remote_directory, file_server_ip = get_share_mount_details()
            self.create_fileserver_connection(protocol_type, host, port, user, password, share_name, mount_drive, file_server_ip)
            listing, full_path = self.list_directories(protocol_type=protocol_type, host=host, share_name=share_name)
            logging.info("Directory List : {}".format(listing))

            input_file_layout, file_patterns, file_name_mnemonic = get_input_file_layout_patterns()
            logging.info("Input File Layout : {}".format(input_file_layout))
            logging.info("File Patterns : {}".format(file_patterns))
            logging.info("File Mnemonic : {}".format(file_name_mnemonic))

            file_extension, original_directory_hierarchy, archive_directory_hierarchy, add_date_suffix, delimiter, quote_char, encoding_scheme, quoting, escape_char, header_record = get_file_layout_details(input_file_layout)
            logging.info("File Extension : {}".format(file_extension))
            logging.info("Original Directory Hierarchy : {}".format(original_directory_hierarchy))
            logging.info("Archive Directory Hierarchy : {}".format(archive_directory_hierarchy))
            logging.info("Add Date Suffix : {}".format(add_date_suffix))

            original_directory_path = qualify_directory(original_directory_hierarchy, folder=src_folder, source_system=sourcesysnm, process_group=process_group, file_type=file_extension)
            logging.info("Original Directory Path : {}".format(original_directory_path))

            if process_group == None:
                remote_directory_path = remote_directory
            else:
                remote_directory_path = os.path.join(remote_directory, process_group)

            listing_remote_dir, full_path =  self.list_directories(protocol_type=protocol_type, directory=remote_directory_path, host=host, share_name=share_name)
            logging.info("Files in Directory : {}".format(listing_remote_dir))

            self.initialize_file_info()
            file_instance_dict = {}
            file_instance_dict['file_extension'] = file_extension
            file_instance_dict['process_group'] = process_group

            for pattern in file_patterns:
                pattern_code, pattern_fmt_code, pattern_prefix, source_extension, archive_method = get_file_pattern_details(pattern)
                logging.info(pattern_fmt_code)
                file_name_pattern = re.sub(r"[^a-zA-Z0-9_-]+", '', pattern_fmt_code.split('.')[0])
                self.set_file_name_pattern(file_name_pattern)
                objectfullpath = '{}{}'.format(original_directory_path, pattern_prefix)
                blobs = get_blobs_list_using_prefix(bq_project_id, stnd_gcs_bucket, objectfullpath)

                for blob in blobs:
                    gcs_object_path = blob.name
                    logging.info(gcs_object_path)
                    is_pattern = re.search(pattern_fmt_code, gcs_object_path, re.IGNORECASE)
                    if is_pattern:
                        logging.info("Path matching regex {}".format(gcs_object_path))

                        file_name = gcs_object_path[gcs_object_path.rindex("/")+1:]
                        remote_location, file_location = self.set_file_locations(file_name, remote_directory_path, df_base_dir)
                        # file_location = self.set_local_file_location(file_name, df_base_dir)
                        # file_name_only = file_name.replace("." + file_extension, "")
                        # logging.info("File Name Only {}".format(file_name_only))
                        
                        match process_type.lower():
                            case "put_files":
                                download_object_to_file(bq_project_id, stnd_gcs_bucket, gcs_object_path, file_location)
                                self.set_file_name_line_count(file_name_pattern, file_name, file_location, pattern_code, file_extension)
                                self.upload_remote_file(protocol_type, remote_location, file_location, host, share_name)
                                removelocalfile(file_location)
            
            listing, full_path = self.list_directories(protocol_type=protocol_type, host=host, share_name=share_name)
            logging.info("Directory List : {}".format(listing))

            listing_remote_dir, full_path =  self.list_directories(protocol_type=protocol_type, directory=remote_directory_path, host=host, share_name=share_name)
            logging.info("Files in Directory : {}".format(listing_remote_dir))
            self.check_patterns_found(file_patterns)

        except:
            logging.error(
                "===ERROR: Failure occurred within fileserver_put_files function===")
            logging.error(traceback.format_exc())
            raise SystemExit()
        
        self.close_fileserver_connection(protocol_type, host)
        logging.info("Connection Closed!")
        logging.info(self.file_details)
        return self.file_details

    def create_outbound_files(self):
        logging.info("==== In create_outbound_files ====")
        
        try:
            tableload_start_time = str(pendulum.now(current_timezone))[:26]

            output_file_layout, output_file_pattern, file_patterns = get_output_file_layout_patterns()
            logging.info("Output File Layout : {}".format(output_file_layout))
            logging.info("Output File Pattern : {}".format(output_file_pattern))
            logging.info("File Patterns : {}".format(file_patterns))
            
            file_extension, original_directory_hierarchy, archive_directory_hierarchy, add_date_suffix, delimiter, quote_char, encoding_scheme, quoting, escape_char, header_record = get_file_layout_details(output_file_layout)
            logging.info("File Extension : {}".format(file_extension))
            logging.info("Original Directory Hierarchy : {}".format(original_directory_hierarchy))
            logging.info("Archive Directory Hierarchy : {}".format(archive_directory_hierarchy))
            logging.info("Add Date Suffix : {}".format(add_date_suffix))

            original_directory_path = qualify_directory(original_directory_hierarchy, folder=src_folder, source_system=sourcesysnm, process_group=process_group, file_type=file_extension)
            logging.info("Original Directory Path : {}".format(original_directory_path))

            archived_directory_path = qualify_directory(archive_directory_hierarchy, folder=arcv_folder, source_system=sourcesysnm, process_group=process_group, file_type=file_extension)
            logging.info("Archived Directory Path : {}".format(archived_directory_path))
    
            self.initialize_file_info()
            for pattern in file_patterns:
                output_file_name, sql_template_file_name, column_names = get_output_file_pattern_details(pattern, file_extension, output_file_pattern)
                self.set_file_name_pattern(output_file_name)
                logging.info("Output File Name {}".format(output_file_name))
                objectfullpath = '{}{}'.format(original_directory_path, output_file_name)

                logging.info("SQL Template File {}".format(sql_template_file_name))
                sql_template_contents = get_template_contents(sql_template_file_name)
                sql_dict = {'params': get_bq_params(bq_project_id)}
                sql_dict.update({"operational_group": process_group})

                results_df = execute_bq_query_from_template(bq_project_id, sql_template_contents, sql_dict, "Select")
                if not results_df.empty:
                    results_df.columns = column_names
                    gcs_object_path = upload_object_from_dataframe(bq_project_id, stnd_gcs_bucket, objectfullpath, results_df, delimiter, quote_char, encoding_scheme, quoting, escape_char, header_record)
                else:
                    logging.info("Empty Dataset for template {}".format(sql_template_file_name))
        except:
            logging.error(
                "===ERROR: Failure occurred within create_outbound_files function===")
            logging.error(traceback.format_exc())
            raise SystemExit()

        logging.info(self.file_details)
        return self.file_details

    def archive_files(self):
        logging.info("==== In archive_files ====")
        
        try:
            tableload_start_time = str(pendulum.now(current_timezone))[:26]

            input_file_layout, file_patterns, file_name_mnemonic = get_input_file_layout_patterns()
            logging.info("Input File Layout : {}".format(input_file_layout))
            logging.info("File Patterns : {}".format(file_patterns))
            logging.info("File Mnemonic : {}".format(file_name_mnemonic))
            
            file_extension, original_directory_hierarchy, archive_directory_hierarchy, add_date_suffix, delimiter, quote_char, encoding_scheme, quoting, escape_char, header_record = get_file_layout_details(input_file_layout)
            logging.info("File Extension : {}".format(file_extension))
            logging.info("Original Directory Hierarchy : {}".format(original_directory_hierarchy))
            logging.info("Archive Directory Hierarchy : {}".format(archive_directory_hierarchy))
            logging.info("Add Date Suffix : {}".format(add_date_suffix))

            original_directory_path = qualify_directory(original_directory_hierarchy, folder=src_folder, source_system=sourcesysnm, process_group=process_group, file_type=file_extension)
            logging.info("Original Directory Path : {}".format(original_directory_path))

            archived_directory_path = qualify_directory(archive_directory_hierarchy, folder=arcv_folder, source_system=sourcesysnm, process_group=process_group, file_type=file_extension)
            logging.info("Archived Directory Path : {}".format(archived_directory_path))
    
            self.initialize_file_info()

            for pattern in file_patterns:
                pattern_code, pattern_fmt_code, pattern_prefix, source_extension, archive_method = get_file_pattern_details(pattern)
                self.set_file_name_pattern(pattern_prefix)                
                objectfullpath = '{}{}'.format(original_directory_path, pattern_prefix)
                blobs = get_blobs_list_using_prefix(bq_project_id, stnd_gcs_bucket, objectfullpath)
                for blob in blobs:
                    gcs_object_path = blob.name
                    logging.info(gcs_object_path)
                    is_pattern = re.search(pattern_fmt_code, gcs_object_path, re.IGNORECASE)
                    if is_pattern:
                        logging.info("Path matching regex {}".format(gcs_object_path))

                        file_name = gcs_object_path[gcs_object_path.rindex("/")+1:]
                        file_location = self.set_local_file_location(file_name, df_base_dir)
                        file_name_only = file_name.replace("." + file_extension, "")
                        logging.info("File Name Only {}".format(file_name_only))

                        download_object_to_file(bq_project_id, stnd_gcs_bucket, gcs_object_path, file_location)
                        if add_date_suffix == 'Yes':
                            if archive_method.lower() == 'gzip':
                                archive_file_name = f"{file_name_only}_{pendulum.now(current_timezone).strftime('%Y%m%d')}.{file_extension}.gz"
                            else:
                                archive_file_name = f"{file_name_only}_{pendulum.now(current_timezone).strftime('%Y%m%d')}.{file_extension}"
                        else:
                            if archive_method.lower() == 'gzip':
                                archive_file_name = f"{file_name}.gz"
                            else:
                                archive_file_name = f"{file_name}"
                        archive_file_location = self.set_local_file_location(archive_file_name, df_base_dir)
                        objectfullpath = '{}{}'.format(archived_directory_path, archive_file_name)
                        archive_gcs_file(bq_project_id, arcv_gcs_bucket, objectfullpath, file_location, archive_file_location, archive_method, file_extension)
                        removelocalfile(file_location)
                        removelocalfile(archive_file_location)
                        removegcsfile(bq_project_id, stnd_gcs_bucket, gcs_object_path)
        except:
            logging.error(
                "===ERROR: Failure occurred within archive_files function===")
            logging.error(traceback.format_exc())
            raise SystemExit()

        logging.info(self.file_details)
        return self.file_details


    def convert_files(self):
        import xlsxwriter
        logging.info("==== In convert_files ====")
        
        try:
            tableload_start_time = str(pendulum.now(current_timezone))[:26]

            input_file_layout, file_name_mnemonic, conversion_type, output_file_layout, output_file_pattern, file_patterns = get_file_conversion_details()
            logging.info("Input File Layout : {}".format(input_file_layout))
            logging.info("Output File Layout : {}".format(output_file_layout))
            logging.info("Conversion Type : {}".format(conversion_type))
            logging.info("Output File Pattern : {}".format(output_file_pattern))
            logging.info("File Mnemonic : {}".format(file_name_mnemonic))
            logging.info("File Patterns : {}".format(file_patterns))

            match conversion_type.lower():
                case "excel_to_csv":
                    sheet_names = get_excel_csv_conversion_details()
                    logging.info("Sheet Names : {}".format(sheet_names))
                case "csv_to_excel":
                    output_worksheet_pattern = get_csv_excel_conversion_details()
                    logging.info("Sheet Pattern : {}".format(output_worksheet_pattern))
            
            input_file_extension, input_original_directory_hierarchy, input_archive_directory_hierarchy, input_add_date_suffix, input_delimiter, input_quote_char, input_encoding_scheme, input_quoting, input_escape_char, input_header_record = get_file_layout_details(input_file_layout)
            logging.info("Input File Extension : {}".format(input_file_extension))
            logging.info("Input Original Directory Hierarchy : {}".format(input_original_directory_hierarchy))
            logging.info("Input Archive Directory Hierarchy : {}".format(input_archive_directory_hierarchy))
            logging.info("Input Add Date Suffix : {}".format(input_add_date_suffix))

            input_original_directory_path = qualify_directory(input_original_directory_hierarchy, folder=src_folder, source_system=sourcesysnm, process_group=process_group, file_type=input_file_extension)
            logging.info("Input Original Directory Path : {}".format(input_original_directory_path))

            archived_directory_path = qualify_directory(input_archive_directory_hierarchy, folder=arcv_folder, source_system=sourcesysnm, process_group=process_group, file_type=input_file_extension)
            logging.info("Input Archived Directory Path : {}".format(archived_directory_path))
            
            output_file_extension, output_original_directory_hierarchy, output_archive_directory_hierarchy, output_add_date_suffix, output_delimiter, output_quote_char, output_encoding_scheme, output_quoting, output_escape_char, output_header_record = get_file_layout_details(output_file_layout)
            logging.info("Output File Extension : {}".format(output_file_extension))
            logging.info("Output Original Directory Hierarchy : {}".format(output_original_directory_hierarchy))

            output_original_directory_path = qualify_directory(output_original_directory_hierarchy, folder=src_folder, source_system=sourcesysnm, process_group=process_group, file_type=output_file_extension)
            logging.info("Output Original Directory Path : {}".format(output_original_directory_path))

            match conversion_type.lower():
                case "csv_to_excel":
                    file_instance_dict = {}
                    file_instance_dict['file_extension'] = output_file_extension
                    file_instance_dict['process_group'] = process_group
                    output_file_name = qualify_file_pattern(output_file_pattern, file_instance_dict)
                    logging.info("Output File Name {}".format(output_file_name))

                    output_file_location = self.set_local_file_location(output_file_name, df_base_dir)
                    dest_objectfullpath = '{}{}'.format(output_original_directory_path, output_file_name)

                    write_mode = "w"
                    writer = pd.ExcelWriter(output_file_location, mode=write_mode, engine='xlsxwriter')
                    csv_files = 0

            self.initialize_file_info()


            for pattern in file_patterns:
                pattern_code, pattern_fmt_code, pattern_prefix, source_extension, input_archive_method = get_file_pattern_details(pattern)
                self.set_file_name_pattern(pattern_prefix)                
                objectfullpath = '{}{}'.format(input_original_directory_path, pattern_prefix)
                blobs = get_blobs_list_using_prefix(bq_project_id, stnd_gcs_bucket, objectfullpath)
                for blob in blobs:
                    gcs_object_path = blob.name
                    logging.info(gcs_object_path)
                    is_pattern = re.search(pattern_fmt_code, gcs_object_path, re.IGNORECASE)
                    if is_pattern:
                        logging.info("Path matching regex {}".format(gcs_object_path))

                        file_name = gcs_object_path[gcs_object_path.rindex("/")+1:]
                        file_location = self.set_local_file_location(file_name, df_base_dir)
                        file_name_only = file_name.replace("." + input_file_extension, "")
                        logging.info("File Name Only {}".format(file_name_only))
                        
                        done_file_pattern = get_done_file_pattern(pattern)
                        logging.info("Done File Pattern {}".format(done_file_pattern))

                        if done_file_pattern:
                            file_instance_dict = {}
                            file_instance_dict['file_name_only'] = file_name_only
                            file_instance_dict['file_extension'] = input_file_extension
                            file_instance_dict['process_group'] = process_group
                            file_instance_dict['file_prefix'] = pattern_prefix
                            done_file_name = qualify_file_pattern(done_file_pattern, file_instance_dict)
                            objectfullpath = '{}{}'.format(input_original_directory_path, done_file_name)
                            logging.info("Done File Path {}".format(objectfullpath))
                            blob_exists = check_object_exists(bq_project_id, stnd_gcs_bucket, objectfullpath)
                            if not blob_exists:
                                logging.error(
                                    "===ERROR: Failure occurred within load_files_to_bigquery function===")
                                logging.error(
                                    f"===ERROR: Done File {done_file_name} not found in path {objectfullpath} ===")
                                raise SystemExit()

                        download_object_to_file(bq_project_id, stnd_gcs_bucket, gcs_object_path, file_location)

                        output_file_instance_dict = {}
                        input_file_instance_dict = {}

                        match conversion_type.lower():
                            case "excel_to_csv":
                                file_instance_dict = {}
                                file_instance_dict['file_name_only'] = file_name_only
                                file_instance_dict['file_extension'] = output_file_extension
                                file_instance_dict['process_group'] = process_group
                                file_instance_dict['file_prefix'] = pattern_prefix
                                output_file_name = qualify_file_pattern(output_file_pattern, file_instance_dict)
                                logging.info("Output File Name {}".format(output_file_name))

                                output_file_location = self.set_local_file_location(output_file_name, df_base_dir)
                                dest_objectfullpath = '{}{}'.format(output_original_directory_path, output_file_name)

                                output_file_instance_dict['delimiter'] = output_delimiter
                                output_file_instance_dict['quote_char'] = output_quote_char
                                output_file_instance_dict['encoding_scheme'] = output_encoding_scheme
                                output_file_instance_dict['quoting'] = output_quoting
                                output_file_instance_dict['escape_char'] = output_escape_char
                                output_file_instance_dict['header_record'] = output_header_record
                                input_file_instance_dict['sheet_names'] = sheet_names
                            case "csv_to_excel":
                                input_file_instance_dict['delimiter'] = input_delimiter
                                input_file_instance_dict['quote_char'] = input_quote_char
                                input_file_instance_dict['encoding_scheme'] = input_encoding_scheme
                                input_file_instance_dict['quoting'] = input_quoting
                                input_file_instance_dict['escape_char'] = input_escape_char
                                input_file_instance_dict['header_record'] = input_header_record
                                input_file_instance_dict['file_name_only'] = file_name_only
                                input_file_instance_dict['file_prefix'] = pattern_prefix
                                output_file_instance_dict['output_worksheet_pattern'] = output_worksheet_pattern
                                output_file_instance_dict['writer'] = writer
                                csv_files += 1

                        convert_file(bq_project_id, stnd_gcs_bucket, dest_objectfullpath, file_location, output_file_location, output_file_extension, conversion_type, input_file_instance_dict, output_file_instance_dict)

                        if input_add_date_suffix == 'Yes':
                            if input_archive_method.lower() == 'gzip':
                                archive_file_name = f"{file_name_only}_{pendulum.now(current_timezone).strftime('%Y%m%d')}.{input_file_extension}.gz"
                            else:
                                archive_file_name = f"{file_name_only}_{pendulum.now(current_timezone).strftime('%Y%m%d')}.{input_file_extension}"
                        else:
                            if input_archive_method.lower() == 'gzip':
                                archive_file_name = f"{file_name}.gz"
                            else:
                                archive_file_name = f"{file_name}"
                        archive_file_location = self.set_local_file_location(archive_file_name, df_base_dir)
                        objectfullpath = '{}{}'.format(archived_directory_path, archive_file_name)
                        archive_gcs_file(bq_project_id, arcv_gcs_bucket, objectfullpath, file_location, archive_file_location, input_archive_method, input_file_extension)
                        removelocalfile(file_location)
                        match conversion_type.lower():
                            case "excel_to_csv":
                                removelocalfile(output_file_location)
                        removelocalfile(archive_file_location)
                        removegcsfile(bq_project_id, stnd_gcs_bucket, gcs_object_path)
            match conversion_type.lower():
                case "csv_to_excel":
                    writer.close()
                    if csv_files > 0:
                        upload_object_from_file(bq_project_id, stnd_gcs_bucket, dest_objectfullpath, output_file_location, output_file_extension)
                    removelocalfile(output_file_location)
        except:
            logging.error(
                "===ERROR: Failure occurred within convert_files function===")
            logging.error(traceback.format_exc())
            raise SystemExit()

        logging.info(self.file_details)
        return self.file_details

    def load_files_to_bigquery(self):
        logging.info("==== In load_files_to_bigquery ====")
        
        try:
            tableload_start_time = str(pendulum.now(current_timezone))[:26]

            logging.info("SQL List : {}".format(sql_list))

            pre_sql_options_individual_files, pre_sql_options_processes, post_sql_options_individual_files, post_sql_options_processes = separate_sql_options(sql_list)
            logging.info("Pre SQL List - Every Record : {}".format(pre_sql_options_individual_files))
            logging.info("Pre SQL List - Entire Process : {}".format(pre_sql_options_processes))
            logging.info("Post SQL List - Every Record : {}".format(post_sql_options_individual_files))
            logging.info("Post SQL List - Entire Process : {}".format(post_sql_options_processes))

            file_id, input_file_layout, file_name_mnemonic, file_patterns, output_table_name, preprocessing_combination, missing_fields_position_combination_list, remove_field_positions, reformat_field_positions, tolerance_percent = get_file_load_details()
            logging.info("File ID : {}".format(file_id))
            logging.info("Input File Layout : {}".format(input_file_layout))
            logging.info("File Mnemonic : {}".format(file_name_mnemonic))
            logging.info("File Patterns : {}".format(file_patterns))
            logging.info("Table Load : {}".format(output_table_name))
            logging.info("Preprocessing Combination : {}".format(preprocessing_combination))
            logging.info("Missing Fields Position Combination List : {}".format(missing_fields_position_combination_list))
            logging.info("Remove Field Positions : {}".format(remove_field_positions))
            logging.info("Reformat Time Field Positions : {}".format(reformat_field_positions))
            logging.info("Tolerance Percent : {}".format(tolerance_percent))

            file_extension, original_directory_hierarchy, archive_directory_hierarchy, add_date_suffix, delimiter, quote_char, encoding_scheme, quoting, escape_char, header_record = get_file_layout_details(input_file_layout)
            logging.info("File Extension : {}".format(file_extension))
            logging.info("Original Directory Hierarchy : {}".format(original_directory_hierarchy))
            logging.info("Archive Directory Hierarchy : {}".format(archive_directory_hierarchy))
            logging.info("Add Date Suffix : {}".format(add_date_suffix))
            logging.info("Delimiter : {}".format(delimiter))
            logging.info("Quote Char : {}".format(quote_char))
            logging.info("Encoding Scheme : {}".format(encoding_scheme))
            logging.info("Quoting : {}".format(quoting))
            logging.info("Escape Char : {}".format(escape_char))
            logging.info("Header Record : {}".format(header_record))

            original_directory_path = qualify_directory(original_directory_hierarchy, folder=src_folder, source_system=sourcesysnm, process_group=process_group, file_type=file_extension)
            logging.info("Original Directory Path : {}".format(original_directory_path))

            archived_directory_path = qualify_directory(archive_directory_hierarchy, folder=arcv_folder, source_system=sourcesysnm, process_group=process_group, file_type=file_extension)
            logging.info("Archived Directory Path : {}".format(archived_directory_path))

            preprocessing_steps = get_preprocessing_steps(preprocessing_combination)
            logging.info("Preprocessing Steps : {}".format(preprocessing_steps))

            target_table_name = get_resolved_table_name(bq_project_id, output_table_name)

            self.initialize_file_info()

            target_file_count = 0

            for pattern in file_patterns:
                pattern_code, pattern_fmt_code, pattern_prefix, source_extension, archive_method = get_file_pattern_details(pattern)

                self.set_file_name_pattern(pattern_prefix)                
                objectfullpath = '{}{}'.format(original_directory_path, pattern_prefix)
                blobs = get_blobs_list_using_prefix(bq_project_id, stnd_gcs_bucket, objectfullpath)
                for blob in blobs:
                    gcs_object_path = blob.name
                    logging.info(gcs_object_path)
                    is_pattern = re.search(pattern_fmt_code, gcs_object_path, re.IGNORECASE)
                    if is_pattern:
                        logging.info("Path matching regex {}".format(gcs_object_path))

                        target_file_count = target_file_count + 1

                        file_name = gcs_object_path[gcs_object_path.rindex("/")+1:]
                        file_location = self.set_local_file_location(file_name, df_base_dir)
                        file_name_only = file_name.replace("." + file_extension, "")
                        logging.info("File Name Prefix {}".format(file_name_only))

                        download_object_to_file(bq_project_id, stnd_gcs_bucket, gcs_object_path, file_location)
                        # expected_count = count_records_in_csv(file_location, encoding_scheme)
                        
                        done_file_pattern = get_done_file_pattern(pattern)
                        logging.info("Done File Pattern {}".format(done_file_pattern))

                        if done_file_pattern:
                            file_instance_dict = {}
                            file_instance_dict['file_name_only'] = file_name_only
                            file_instance_dict['file_extension'] = file_extension
                            file_instance_dict['process_group'] = process_group
                            file_instance_dict['file_prefix'] = pattern_prefix                        
                            done_file_name = qualify_file_pattern(done_file_pattern, file_instance_dict)
                            objectfullpath = '{}{}'.format(original_directory_path, done_file_name)
                            logging.info("Done File Path {}".format(objectfullpath))
                            blob_exists = check_object_exists(bq_project_id, stnd_gcs_bucket, objectfullpath)
                            if not blob_exists:
                                logging.error(
                                    "===ERROR: Failure occurred within load_files_to_bigquery function===")
                                logging.error(
                                    f"===ERROR: Done File {done_file_name} not found in path {objectfullpath} ===")
                                raise SystemExit()
                        
                        file_instance_dict = {}
                        file_instance_dict['file_name_only'] = file_name_only
                        file_instance_dict['file_extension'] = file_extension
                        file_instance_dict['source_extension'] = source_extension
                        file_instance_dict['process_group'] = process_group
                        file_instance_dict['file_prefix'] = pattern_prefix
                        if reformat_field_positions is not None:
                            file_instance_dict['reformat_field_positions'] = reformat_field_positions
                        if remove_field_positions is not None:
                            file_instance_dict['remove_field_positions'] = remove_field_positions
                        if missing_fields_position_combination_list is not None:
                            file_instance_dict['missing_fields_position_combination_list'] = missing_fields_position_combination_list
                        
                        file_contents, expected_count_with_bad_record_count, expected_count = preprocess_step_execution(preprocessing_steps, file_location, encoding_scheme, delimiter, quote_char, file_instance_dict)
                        
                        actual_count = load_file_to_bigquery(bq_project_id, file_location, encoding_scheme, target_table_name, file_extension, delimiter, quote_char)
                        
                        if post_sql_options_individual_files:
                            for post_sql_options_individual_file in post_sql_options_individual_files:
                                sql_dict = {'params': get_bq_params(bq_project_id)}
                                match post_sql_options_individual_file['sql_template_file_name']:
                                    case "insert_audit_control.j2":
                                        tableload_end_time = str(pendulum.now(current_timezone))[:26]
                                        tableload_run_time = (pd.to_datetime(tableload_end_time) - pd.to_datetime(tableload_start_time))
                                        expected_value = expected_count
                                        actual_value = actual_count
                                        
                                        if expected_value == actual_value:
                                            audit_status = 'PASS'
                                        elif actual_value > expected_value:
                                            audit_status = 'PASS(More records in Target)'
                                        else:
                                            if tolerance_percent != 0 and expected_value != 0:
                                                var_pct = round((abs(actual_value-expected_value)/expected_value) * 100,2)
                                                logging.info("Variance Percentage ", var_pct)
                                                if var_pct > tolerance_percent:
                                                    audit_status = 'FAIL'
                                                else:
                                                    audit_status = 'PASS(Less records in target but meets tolerance)'
                                            else:
                                                audit_status = 'FAIL'
                                                
                                        table_name = target_table_name.replace(bq_project_id + ".", "")
                                        sql_dict.update({ 
                                                        "table_id": file_id, "source_system": sourcesysnm, 
                                                        "file_name": f"{file_name_only}.{source_extension}", "table_name": table_name,  
                                                        "expected_value": expected_count, "actual_value": actual_count,  
                                                        "tableload_start_time": tableload_start_time, "tableload_end_time": tableload_end_time,  
                                                        "tableload_run_time": tableload_run_time, "job_name": jobname.replace(".","-"),  
                                                        "audit_status": audit_status
                                                        })
                                        execute_post_processing_queries(bq_project_id, post_sql_options_individual_file, sql_dict)

                                    case "insert_excel_audit.j2":
                                        sql_dict.update({
                                                        "file_name_mnemonic": file_name_mnemonic, "operational_group": process_group,
                                                        "file_name": f"{file_name_only}.{source_extension}",
                                                        "all_src_rec_count": expected_count_with_bad_record_count,
                                                        "target_load_count": f"{ 'NULL' if actual_count == 0 else str(actual_count) }"
                                                        })
                                        execute_post_processing_queries(bq_project_id, post_sql_options_individual_file, sql_dict)

                                    case "insert_target_filenames.j2":
                                        sql_dict.update({
                                                        "file_name_mnemonic": file_name_mnemonic, "operational_group": process_group,
                                                        "file_path": f"{stnd_gcs_bucket}/{gcs_object_path.replace(file_extension,source_extension)}",
                                                        "file_name": f"{file_name_only}.{source_extension}"
                                                        })
                                        execute_post_processing_queries(bq_project_id, post_sql_options_individual_file, sql_dict)

                                    case "insert_gl_reconciliation_error_log.j2":
                                        if actual_count == 0:
                                            sql_dict.update({ 
                                                            "file_name": f"{file_name_only}.{source_extension}",
                                                            "file_path": f"{stnd_gcs_bucket}/{original_directory_path.replace(file_extension,source_extension)}",
                                                            "co_id": f"{file_name_only[-5:]}",
                                                            "description": f"{ 'Excel File is empty' if expected_count_with_bad_record_count == 0 else 'COID is empty for the rows present in excel' }",
                                                            "operational_group": process_group
                                                            })
                                            execute_post_processing_queries(bq_project_id, post_sql_options_individual_file, sql_dict)
                                
                        if add_date_suffix == 'Yes':
                            if archive_method.lower() == 'gzip':
                                archive_file_name = f"{file_name_only}_{pendulum.now(current_timezone).strftime('%Y%m%d')}.{file_extension}.gz"
                            else:
                                archive_file_name = f"{file_name_only}_{pendulum.now(current_timezone).strftime('%Y%m%d')}.{file_extension}"
                        else:
                            if archive_method.lower() == 'gzip':
                                archive_file_name = f"{file_name}.gz"
                            else:
                                archive_file_name = f"{file_name}"
                        archive_file_location = self.set_local_file_location(archive_file_name, df_base_dir)
                        objectfullpath = '{}{}'.format(archived_directory_path, archive_file_name)
                        archive_gcs_file(bq_project_id, arcv_gcs_bucket, objectfullpath, file_location, archive_file_location, archive_method, file_extension)
                        removelocalfile(archive_file_location)
                        removelocalfile(file_location)
                        removegcsfile(bq_project_id, stnd_gcs_bucket, gcs_object_path)
            if post_sql_options_processes:
                for post_sql_options_process in post_sql_options_processes:
                    sql_dict = {'params': get_bq_params(bq_project_id)}
                    match post_sql_options_process['sql_template_file_name']:
                        case "update_folder_audit.j2":
                            sql_dict.update({"file_name_mnemonic": file_name_mnemonic, "operational_group": process_group,
                                             "target_folder_path": f"{stnd_gcs_bucket}/{original_directory_path}",
                                             "table_file_count": target_file_count})
                    execute_post_processing_queries(bq_project_id, post_sql_options_process, sql_dict)
        except:
            logging.error(
                "===ERROR: Failure occurred within load_files_to_bigquery function===")
            logging.error(traceback.format_exc())
            raise SystemExit()

        logging.info(self.file_details)
        return self.file_details

    def process(self, context):

        try:
            process_start_time = pendulum.now(current_timezone).to_datetime_string()
            logging.info('Fileserver Processing started at {}'.format(process_start_time))
            logging.info("Process Type : {}".format(process_type))
            match process_type.lower():
                case process if process in ["get_files", "delete_files"]: 
                    file_details = self.fileserver_get_delete_files()
                case "put_files":
                    file_details = self.fileserver_put_files()
                case "outbound_files":
                    file_details = self.create_outbound_files()
                case "archive_files":
                    file_details = self.archive_files()
                case "load_files":
                    file_details = self.load_files_to_bigquery()
                case "convert_files":
                    file_details = self.convert_files()

            process_end_time = pendulum.now(current_timezone).to_datetime_string()
            logging.info('Fileserver Processing completed at {}'.format(process_end_time))

            # return file_details

        except:
            logging.error(
                "===ERROR: Failure occurred within Process function===")
            logging.error(traceback.format_exc())
            raise SystemExit()

def run():
    pipeline_args = [
        "--project", config['env']['v_proc_project_id'],
        "--service_account_email",  config['env']['v_serviceaccountemail'],
        "--job_name", jobname.replace(".","-"),
        "--runner", beam_runner,
        "--network", config['env']["v_network"],
        "--subnetwork", config['env']["v_subnetwork"],
        "--staging_location", config['env']["v_dfstagebucket"],
        "--temp_location", config['env']["v_gcs_temp_bucket"],
        "--region", config['env']["v_region"],
        "--save_main_session",
        "--num_workers", str(config['env']["v_numworkers"]),
        "--max_num_workers",   str(config['env']["v_maxworkers"]),
        "--no_use_public_ips",
        # "--dataflow_service_options, enable_prime",
        "--autoscaling_algorithm", "THROUGHPUT_BASED",
        "--worker_machine_type", machine_type,
        "--setup_file", '{}setup.py'.format(utilpath)

    ]

    try:
        logging.info(
            "===Apache Beam Run with pipeline options started===")
        pcoll = beam.Pipeline(argv=pipeline_args)

        logging.info(pipeline_args)
        
        # use /tmp/ on dataflow worker node for processing
        global df_base_dir
        df_base_dir = tempfile.gettempdir()
        logging.info("Temp Directory : {}".format(df_base_dir))

        global stnd_gcs_bucket
        stnd_gcs_bucket = config['env']['v_data_bucket_name']

        global arcv_gcs_bucket
        arcv_gcs_bucket = config['env']['v_data_archive_bucket_name']

        global src_folder
        src_folder = config['env']['v_srcfilesdir']

        global arcv_folder
        arcv_folder = config['env']['v_archivedir']

        global tgt_folder
        tgt_folder = config['env']['v_tgtfilesdir']

        global schema_folder
        tmp_folder = config['env']['v_tmpobjdir']
        schema_folder = tmp_folder + config['env']['v_schemasubdir']

        global dag_gcs_bucket
        dag_gcs_bucket = config['env']['v_dag_bucket_name']

        global gcs_audit_notification_folder
        gcs_audit_notification_folder =  f"dags/{lob}/{config['env']['v_dag_audit_notification_path']}"

        match process_type.lower():
            case "get_files":
                process_desc = "Copy Files from File Server to GCS"
            case "convert_files":
                process_desc = "Convert Files from one format to another"
            case "load_files":
                process_desc = "Load Files from GCS to BigQuery"
            case "archive_files":
                process_desc = "Archive Files within GCS"
            case "delete_files":
                process_desc = "Delete Files from File Server"
            case "put_files":
                process_desc = "Upload Files to File Server from GCS"
            case "outbound_files":
                process_desc = "Execute Queries and store in GCS"
        
        pcoll | "Initialize" >> beam.Create(["1"]) | process_desc >> beam.ParDo(file_processing()) 
                # | "Read GCS, Write to Google BigQuery" >> beam.ParDo(gcstobq())

        p = pcoll.run()
        logging.info("===Apache Beam Run completed successfully===")
        if beam_runner == 'DataflowRunner':
            dataflow_job_id = p.job_id()
            logging.info( "===Submitted Asynchronous Dataflow Job, Job id is " + dataflow_job_id + " ====")
        else:
            dataflow_job_id = random.randint(1000000,10000000)
        return dataflow_job_id

    except:
        logging.exception("===ERROR: Apache Beam Run Failed===")
        raise SystemExit()


def main(sourcesysnm, file_name_mnemonic):

    global jobname
    randomstring = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))

    jobname = sourcesysnm[:3] + "-" + process_type.lower().split("_")[0] + "-" + process_group.replace('_', '-').lower() + "-" + file_name_mnemonic.replace('_', '-').lower() + '-' + time.strftime("%Y%m%d%H%M%S") + '-' + randomstring
        
    logging.info("===Job Name is {} ===".format(jobname))
    p1 = run()
    dataflow_job_id = p1

    logging.info("===END: Data Pipeline for Source {} and file prefix {} at {}===".format(
        sourcesysnm, file_name_mnemonic, time.strftime("%Y%m%d-%H:%M:%S")))
    
    # DON'T CHANGE BELOW PRINT TO a LOGGING STATEMENT; THE RETURN VALUE GETS OBSCURED IF BELOW STATEMENT IS A LOGGING STMT
    print(dataflow_job_id)

    return dataflow_job_id


if __name__ == "__main__":
    
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--src_system", required=True,
                        help=("Source System"))
    parser.add_argument("--server", required=False,
                        help=("Server Name"))
    parser.add_argument("--dag_name_suffix", required=True,
                        help=("DAG Name Suffix"))
    parser.add_argument("--file_name_mnemonic", required=True,
                        help=("File Name Mnemonic"))
    parser.add_argument("--process_type", required=True,
                        help=("Process Type"))
    parser.add_argument("--config_type", required=True,
                        help=("Config Type"))
    parser.add_argument("--process_group", required=False,
                        help=("Process Group"))
    parser.add_argument("--beam_runner", required=True,
                        help=("Beam Runner"))
    
    args = parser.parse_args()

    global sourcesysnm
    sourcesysnm = args.src_system

    global file_name_mnemonic
    file_name_mnemonic = args.file_name_mnemonic

    global dag_name_suffix
    dag_name_suffix = args.dag_name_suffix

    global process_type
    process_type = args.process_type

    global config_type
    config_type = args.config_type

    global process_group
    process_group = args.process_group

    global server_name
    server_name = None

    global fileserver_config
    fileserver_config = None

    global machine_type
    machine_type = config['env']['v_worker_machine_type']
    logging.info(machine_type)

    global process_config
    process_config = call_config_yaml(config_folder, f"{lob_abbr}_{sourcesysnm}_{config_type}_config.yaml")

    logging.info(dag_name_suffix)

    process_config_item = next(filter(lambda item: item['dag_name_suffix'] == dag_name_suffix, process_config[sourcesysnm]),None)
    logging.info(process_config_item)

    if process_type.lower() in ["get_files", "delete_files", "put_files"]:
        server_name = args.server
        fileserver_config_item = next(filter(lambda item: item['file_server'] == server_name, process_config_item['file_server_info']),None)
        logging.info(fileserver_config_item)

    match process_type.lower():
        case "convert_files":
            process_depend_config_item = next(filter(lambda item: item['file_name_mnemonic'] == file_name_mnemonic, process_config_item['convert_files']),None)
            logging.info(process_depend_config_item)
        case "load_files":
            process_depend_config_item = next(filter(lambda item: item['file_name_mnemonic'] == file_name_mnemonic, process_config_item['load_files']),None)
            if "load_sql_list" in process_config_item:
                sql_list = process_config_item['load_sql_list']
            else:
                sql_list = None
            logging.info(process_depend_config_item)
        case "archive_files":
            process_depend_config_item = next(filter(lambda item: item['file_name_mnemonic'] == file_name_mnemonic, process_config_item['archive_files']),None)
            logging.info(process_depend_config_item)
        case "outbound_files":
            process_depend_config_item = next(filter(lambda item: item['file_name_mnemonic'] == file_name_mnemonic, process_config_item['outbound_files']),None)
            logging.info(process_depend_config_item)
        case "get_files":
            process_depend_config_item = next(filter(lambda item: item['file_name_mnemonic'] == file_name_mnemonic, process_config_item['get_files']),None)
            if "get_sql_list" in process_config_item:
                sql_list = process_config_item['get_sql_list']
            else:
                sql_list = None
            logging.info(process_depend_config_item)
        case "delete_files":
            process_depend_config_item = next(filter(lambda item: item['file_name_mnemonic'] == file_name_mnemonic, process_config_item['delete_files']),None)
            logging.info(process_depend_config_item)
        case "put_files":
            process_depend_config_item = next(filter(lambda item: item['file_name_mnemonic'] == file_name_mnemonic, process_config_item['put_files']),None)
            logging.info(process_depend_config_item)

    global bq_project_id
    bq_project_id = config['env']['v_curated_project_id']

    global beam_runner
    beam_runner = args.beam_runner

    logging.info(beam_runner)

    logging.info("===BEGIN: Data Pipeline for Source {} and file prefix {} at {}===".format(
        sourcesysnm, file_name_mnemonic, time.strftime("%Y%m%d-%H:%M:%S")))
    main(sourcesysnm, file_name_mnemonic)
    