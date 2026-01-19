from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.api.common.experimental import get_task_instance

import json
import csv
import yaml
import gzip
import io
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

import pgpy

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
                                 deserialize_json=True, description="Saved in dags/config/ folder")
    # config = default_config
    return config


config = call_config_yaml("hrg_config.yaml", "hca_hrg_default_vars")
# get project variables from config file
bq_project_id = config['env']['v_curated_project_id']
bq_staging_dataset = config['env']['v_hr_stage_dataset_name']
stage_dataset = config['env']['v_hr_stage_dataset_name']
gcs_bucket = config['env']['v_data_bucket_name']
src_folder = config['env']['v_srcfilesdir']
arc_folder = config['env']['v_archivedir']
tgt_folder = config['env']['v_tgtfilesdir']
tmp_folder = config['env']['v_tmpobjdir']
schema_folder = tmp_folder + config['env']['v_schemasubdir']

current_timezone = pendulum.timezone("US/Central")

def get_execution_date(execution_date, **kwargs):  
    print("Execution Date - ",execution_date)  
    exec_day = int(execution_date.strftime('%w'))
    print("Execution Day - ",exec_day)
    schedule = kwargs["params"]["schedule"]
    print("Sensor Schedule - ", schedule)
    schedule_days = schedule.split()[4]
    flag=0
    if re.search(",",schedule_days):
        schedule_day=schedule_days.split(",")
        if str(exec_day) in schedule_day:
            if schedule_days=="0,2,3,4,5,6":
                if (exec_day)==0:
                    flag=1
            schedule_day=exec_day
        else:
            logging.info("Dependant DAG is not scheduled to run today")
            raise SystemExit()
    else:
        schedule_day=schedule_days 
    if schedule_day == "*":
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
    if flag==1:
        poke_date = execution_date + timedelta(days=1)
        print("The New Execution Date is ",poke_date)
    print(poke_date.year)
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


def filetodf(src_folder_path, srcfilename, delimiter, header, quotechar, quoting, escape_char, encoding):
    # Read a file into a dataframe
    logging.info("==== In filetodf with params:=====")
    logging.info(src_folder_path, srcfilename, delimiter, header,
                 quotechar, quoting, escape_char, encoding)
    src_file_fullpath = 'gs://' + gcs_bucket + \
        "/" + src_folder_path + "/" + srcfilename

    chunksize = 1000000

    tfr = pd.read_csv(src_file_fullpath,
                      header=header,
                      delimiter=delimiter,
                      quotechar=quotechar,
                      quoting=quoting,
                      # need to test a callable function here
                      on_bad_lines='warn',
                      na_filter=False,
                      escapechar=escape_char,
                      chunksize=chunksize,
                      iterator=True,
                      encoding=encoding,
                      dtype=str
                      )
    df = pd.concat(tfr, ignore_index=True)
    df = df.astype(str)
    df.columns = df.columns.str.lower()
    df_record_count = len(df.index)
    logging.info("File {} read in to dataframe. Row count in dataframe  is {}".format(
        srcfilename, df_record_count))
    return df


def loadbqtable(df, truncate_table, bq_dataset, tgt_bq_table):
    # Load data from a dataframe to big query table
    logging.info("==== In loadbqtable with params====")
    logging.info("Dataframe : {}, Truncate Option : {}, Dataset : {}, Table : {}".format(
        df, truncate_table, bq_dataset, tgt_bq_table))

    # Get table schema
    tblschema = getbqddl(bq_dataset, tgt_bq_table)

    # Build an table column names array
    names = []
    for row in tblschema:
        names.append(row['name'])

    # Change column name and datatype of dataframe
    for i in range(0, len(tblschema)):
        dtype = tblschema[i]['type'].lower()

        if dtype == 'numeric':
            dtype = 'object'
        elif dtype == 'date' or dtype == 'time' or dtype == 'datetime':
            dtype = 'datetime64'
        elif dtype == 'integer':
            dtype = 'int64'

        if i < len(df.columns):
            df = df.rename(columns={df.columns[i]: tblschema[i]['name']})
            df[tblschema[i]['name']] = df[tblschema[i]['name']].astype(dtype)

        else:
            df[tblschema[i]['name']] = None
            df[tblschema[i]['name']] = df[tblschema[i]['name']].astype(dtype)

    logging.info("Data type and column names of the dataframe", df.info())

    tgt_bq_table = bq_dataset + '.' + tgt_bq_table
    if truncate_table == 'truncate':
        pd.read_gbq("truncate table " + tgt_bq_table,
                    project_id=bq_project_id)
        logging.info("Truncated table {}.". format(tgt_bq_table))
    else:
        logging.info("Appending to table {}.". format(tgt_bq_table))

    pandas_gbq.to_gbq(df, tgt_bq_table,
                      project_id=bq_project_id, if_exists='append', table_schema=tblschema)
    logging.info("table load completed.")

    tgt_record_count = pd.read_gbq(
        "select count(1) as count from " + tgt_bq_table, project_id=bq_project_id)
    tgt_record_count = list(tgt_record_count['count'])[0]

    logging.info("Row count in target bq table {} is {}".format(
        tgt_bq_table, tgt_record_count))


def gcstobqfileprocessing(src_folder_path, pre_process, post_process, tgt_bq_table, truncate_table, srcfilename, quotechar, quoting, header, delimiter, escape_char, sourcesysname, encoding):
    # Main function called in DAG to process file to table and archieve files
    logging.info(
        '=========Start GCS to BQ load=================================')

    # Preprocessing to remove unwanted and spl charecters
    if pre_process:
        eval(pre_process+"(src_folder_path,srcfilename,sourcesysname,delimiter,header,quotechar,quoting,escape_char)")

    # Read file to a dataframe
    df = filetodf(src_folder_path, srcfilename, delimiter,
                  header, quotechar, quoting, escape_char, encoding)

    if post_process:
        df = eval(post_process + "(df)")

    # Load data from dataframe to bQ table
    loadbqtable(df, truncate_table, stage_dataset, tgt_bq_table)

    # download file to local
    srcfilelocalname = downloadgcsfile(sourcesysname, srcfilename, encoding)

    # archive file
    archivegcsfile(sourcesysname, srcfilelocalname,
                   srcfilename.split('.')[0], srcfilename.split('.')[1])

    # Remove files
    removegcsfile(sourcesysname, src_folder, srcfilename)
    removelocalfile(srcfilelocalname)

    logging.info(
        '=========End GCS to BQ load=================================')


def mergefiles(blob_list, src_folder_path, delimiter, header, quotechar, quoting, sourcesysname, target_file_name, escape_char, encoding):
    # Merge multiple files and writes to a file
    logging.info("==== In merge_files with params======")
    print(blob_list, src_folder_path, delimiter, header, quotechar,
          quoting, sourcesysname, target_file_name, escape_char, encoding)
    src_folder_full = src_folder_path + sourcesysname
    df_combine = pd.DataFrame()
    filelist = []
    for blob in blob_list:
        blob = blob.split('/')[3]
        filelist.append(blob)
        df = filetodf(src_folder_full, blob, delimiter, header,
                      quotechar, quoting, escape_char, encoding)
        df_combine = pd.concat([df_combine, df], ignore_index=True)

    logging.info("Row count in merged dataframe  is {}".format(
        len(df_combine.index)))

    loadfiletobucket(df_combine, delimiter, quotechar, sourcesysname,
                     src_folder_path, target_file_name, escape_char, quoting)

    for file in filelist:

        # download file to local
        encoding_scheme = "utf-8"
        srcfilelocalname = downloadgcsfile(
            sourcesysname, file, encoding_scheme)

        # archive file
        archivegcsfile(sourcesysname, srcfilelocalname,
                       file.split('.')[0], file.split('.')[1])

        # Remove files
        removegcsfile(sourcesysname, src_folder, file)
        removelocalfile(srcfilelocalname)


def listblobswithpattern(src_folder_path, srcfilename, delimiter, header, quotechar, quoting, sourcesysname, escape_char, merge, encoding):
    # Main Funcation called from dag to match file pattern and merge files if required
    # Check if Regex pattern is passed
    if '$' in srcfilename:
        logging.info("==== In listblobswithpattern wit params====")
        print(src_folder_path, srcfilename, delimiter, header,
              quotechar, quoting, sourcesysname, escape_char, merge)
        bucket = get_bucket(bq_project_id, gcs_bucket)
        storage_client = storage.Client()
        src_folder_prefix = src_folder_path + sourcesysname
        blobs = storage_client.list_blobs(
            bucket, prefix=src_folder_prefix, delimiter=None)
        blob_list = []

        # Find file/s matching pattern
        for blob in blobs:
            is_pattern = re.search(srcfilename, blob.name)

            if is_pattern:
                logging.info("File matching regex {} in bucket {} : {}".format(
                    srcfilename, gcs_bucket, blob.name))
                blob_list.append(blob.name)

        # Merge file if merge is requested
        if len(blob_list) > 1 and merge:
            mergefiles(blob_list, src_folder_path, delimiter, header,
                       quotechar, quoting, sourcesysname, merge, escape_char, encoding)
            merge_file_name = merge
        elif len(blob_list) == 1 and not (merge):
            merge_file_name = blob_list[0].split('/')[3]
        else:
            logging.info("Source File Path is not found for file prefix {} in bucket {}".format(
                srcfilename, gcs_bucket))
    else:
        merge_file_name = srcfilename

    return merge_file_name

def get_gcs_filepath(sourcesysname, source_file_name_prefix):
    # Get Entire GCS Path for an object with prefixed name
    try:
        storage_client = storage.Client(bq_project_id)
        bucket = get_bucket(bq_project_id, gcs_bucket)
        source_file_prefix = '{}{}/{}'.format(src_folder,
                                              sourcesysname, source_file_name_prefix)
        blobs = storage_client.list_blobs(bucket, prefix=source_file_prefix)
        source_file_path = ''
        for blob in blobs:
            source_file_path = blob.name
        logging.info("GCS Path is {}".format(source_file_path))
        return source_file_path
    except:
        logging.info("Source File Path is not found for file prefix {} in bucket {}".format(
            source_file_prefix, gcs_bucket))
        raise SystemExit()


def downloadgcsfile(sourcesysname, srcfilename, encoding_scheme='cp1252'):
    # Download file to directory
    logging.info("===== In downloadgcsfile with params: =====")
    logging.info("Source System : {}, File Name : {}, Encoding : {}".format(
        sourcesysname, srcfilename, encoding_scheme))

    bucket = get_bucket(bq_project_id, gcs_bucket)
    blob = get_blob(bucket, src_folder, sourcesysname, srcfilename)
    blob.download_to_filename(srcfilename)
    if os.path.getsize(srcfilename) == 0:
        logging.error("Empty File, exiting")
        raise SystemExit()
    
    srcfilelocalname = os.path.abspath(srcfilename)

    if encoding_scheme != None:
        src_record_count = sum(1 for _ in enumerate(
            open(srcfilename, encoding=encoding_scheme)))
        logging.info("Downloaded file {}. Row count excluding header is {}".format(
        srcfilelocalname, src_record_count - 1))
    else:
        logging.info("Downloaded file {}. ".format(srcfilelocalname))

    return srcfilelocalname


def loadfiletobucket(df, delimiter, quotechar, sourcesysname, tgt_folder, target_file_name, escape_char, quoting, encoding_scheme='UTF-8'):
    # Load dataframe to landing bucket
    logging.info("==== In loadfiletobucket with params: ====")
    print(df, delimiter, quotechar, sourcesysname, tgt_folder,
          target_file_name, escape_char, quoting, encoding_scheme)
    bucket = get_bucket(bq_project_id, gcs_bucket)
    blob = get_blob(bucket, tgt_folder, sourcesysname, target_file_name)
    blob.upload_from_string(df.to_csv(sep=delimiter, quotechar=quotechar,
                            encoding=encoding_scheme, quoting=quoting, escapechar=escape_char, index=False))
    logging.info("File {} loaded to bucket: {}".format(
        target_file_name, tgt_folder))


def staging_into_outbound_file(**kwargs):
    sourcesysname = kwargs['sourcesysname']
    target_file_name_prefix = kwargs['target_file_name_prefix']
    target_file_extension = kwargs['target_file_extension']
    delimiter = kwargs['delimiter']
    quote_char = kwargs['quote_char']
    is_prefixed_file_name = kwargs['is_prefixed_file_name']
    encoding_scheme = kwargs['encoding_scheme']
    source_query = kwargs['source_query']
    gen_extract_from_bq(source_query, sourcesysname, is_prefixed_file_name,
                        target_file_name_prefix, target_file_extension, delimiter, quote_char, encoding_scheme)

    return "File has been extracted from Staging"


def gen_extract_from_bq(source_query, sourcesysname, is_prefixed_file_name, target_file_name_prefix, target_file_extension, delimiter, quote_char, encoding_scheme):
    # Read data from Source Query and load into Cloud Bucket
    df = pd.read_gbq(source_query, project_id=bq_project_id)

    if is_prefixed_file_name == 'Yes':
        target_file_name = target_file_name_prefix + "_" + \
            datetime.now(current_timezone).strftime(
                '%Y%m%d') + target_file_extension
    else:
        target_file_name = target_file_name_prefix + target_file_extension

    if quote_char == "":
        quoting = csv.QUOTE_NONE
        escape_char = "\\"
    else:
        quoting = csv.QUOTE_ALL
        escape_char = ""

    loadfiletobucket(df, delimiter, quote_char, sourcesysname, tgt_folder,
                     target_file_name, escape_char, quoting, encoding_scheme)
    return None


def upload_object_from_file(sourcesysname, folder, objectname, filepath):
    # Load file from airflow directory to landing bucket
    logging.info("==== In upload_object_from_file with params: ====")
    logging.info("Source System : {}, Folder : {}, File Name : {}, File Path : {}".format(
        sourcesysname, folder, objectname, filepath))
    bucket = get_bucket(bq_project_id, gcs_bucket)
    blob = get_blob(bucket, folder, sourcesysname, objectname)
    blob.upload_from_filename(filepath)
    logging.info("Object {} has been loaded using file {}".format(
        blob.name, filepath))
    return None


def archivegcsfile(sourcesysname, filename, filenameprefix, fileextension):
    # Copy Source File in Archived folder with GZipped format
    logging.info("========== In archivegcsfile params: ============")
    logging.info("Source System : {}, File : {}, File Name Prefix : {}, File Extension : {}".format(
        sourcesysname, filename, filenameprefix, fileextension))
    bucket = get_bucket(bq_project_id, gcs_bucket)

    arcfilename = filenameprefix + '_' + \
        datetime.now(current_timezone).strftime(
            '%Y%m%d') + '.' + fileextension + '.gz'
    with open(filename, 'rb') as src, gzip.open(arcfilename, 'wb') as dst:
        dst.writelines(src)
    blob = get_blob(bucket, arc_folder, sourcesysname, arcfilename)
    blob.upload_from_filename(arcfilename, content_type='application/gzip')
    logging.info("Source file archived at {}".format(blob.name))
    return None
    
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

def removegcsfile(sourcesysname, folder, filename):
    logging.info("===== In removegcsfile params: =====")
    logging.info("Source System : {}, Folder : {}, File Name : {}".format(
        sourcesysname, folder, filename))
    bucket = get_bucket(bq_project_id, gcs_bucket)
    blob = get_blob(bucket, folder, sourcesysname, filename)
    blob.delete()
    logging.info("Source file {}  in GCS removed".format(blob.name))
    return None


def removelocalfile(filenamestring):
    logging.info("==== In removelocalfile params: ====")
    logging.info("File name String : {}".format(filenamestring))
    filelist = glob.glob(filenamestring + '*')
    for file in filelist:
        os.remove(file)
    logging.info("Local file cleanup done")
    return None


def read_contents_file(file_path, encoding_scheme='cp1252'):
    # Read Contents of file
    logging.info("==== In read_contents_file params: ====")
    logging.info("File Path : {}, Encoding Scheme : {}".format(
        file_path, encoding_scheme))
    try:
        input_file = open(file_path, 'r', encoding=encoding_scheme)
        return input_file.read()
    except:
        logging.info("File couldn't be read in path {}".format(file_path))
        raise SystemExit()


def compile_regex_standalonechar(char_to_find, exclude_adjacent_char_list=[], ignore_start_end_occurence=True):
    regex_string = ""
    # Ignore Beginning of File / Line / String
    if ignore_start_end_occurence:
        regex_string += r"(?!^)"
    # Ignore occurrence if preceding letter is in exclude list
    for exclude_adjacent_char in exclude_adjacent_char_list:
        regex_string += "(?<![\\" + exclude_adjacent_char + "])"
    regex_string += "[\\" + char_to_find + "]"
    # Ignore occurrence if succeeding letter is in exclude list
    for exclude_adjacent_char in exclude_adjacent_char_list:
        regex_string += "(?![\\" + exclude_adjacent_char + "])"
    # Ignore End of File / Line / String
    if ignore_start_end_occurence:
        regex_string += r"(?!$)"
    return regex_string


def fix_nonescaped_character_contents(file_contents, delimiter=",", quote_char='"'):
    try:
        # Replace double quote which is not surrounded by delimiter, another double quote, new line, start and end of file
        regex_string = r"(?!^)(?<![\|])(?<![\"])(?<![\n])[\"](?![\|])(?![\"])(?![\n])(?!$)"
        # Replace double quote which is not preceded by delimiter, another double quote, new line and start of file but followed by doublequote and delimiter
        regex_string2 = r"(?!^)(?<![\|])(?<![\"])(?<![\n])[\"](?=\"\|)"
        # Replace double quote which is not succeeded by delimiter, another double quote, new line and end of file but preceded by delimiter and doublequote
        regex_string3 = r"(?<=\|\")[\"](?![\|])(?![\"])(?![\n])(?!$)"
        # regex_string = compile_regex_standalonechar(char_to_find=quote_char, exclude_adjacent_char_list=[delimiter,quote_char,"n"])
        file_contents = re.sub(regex_string, "\"\"", file_contents)
        file_contents = re.sub(regex_string2, "\"\"", file_contents)
        file_contents = re.sub(regex_string3, "\"\"", file_contents)
        logging.info(
            "Content has been corrected for non-escaped double quote")
        return file_contents
    except:
        logging.info(
            "Content couldn't be fixed for nonescaped characters")
        raise SystemExit()

''' 
remove_multiple_headers() function removes multiple headers from the source file

'''
def remove_multiple_headers(file_contents):
    try:
        header_list = file_contents.split("\n", 1)
        header = header_list[0]
        header_list[1] = header_list[1].replace(header,'')
        file_contents = ('\n').join(header_list)
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
correct_date_field() function replaces the Date field in YYYYMMDD format to YYYY-MM-DD format

'''
def correct_date_field(file_contents, field_number, header_rec = True):
    # Replace Date field in YYYYMMDD format to YYYY-MM-DD format
    try:
        lines = file_contents.split("\n")
        file_lines = []
        for index, line in enumerate(lines):
            if line.strip() != '' and (index == 0 and header_rec == True) :
                file_lines.append(line)
            if line.strip() != '' and (index != 0 and header_rec == True) :
                fields = line.split(",",field_number)
                date_field_in_numbers = fields[field_number - 1]
                fields[field_number - 1] = datetime.strptime(date_field_in_numbers,'%Y%m%d').strftime('%Y-%m-%d')
                file_lines.append((',').join(fields))
        file_contents = ('\n').join(file_lines)
        logging.info(
            "Content has been corrected to correct date field")
        return file_contents
    except:
        logging.info(
            "Content couldn't be fixed to correct date field")
        raise SystemExit()

''' 
add_date_field() function adds a column File_Date with current date in YYYY-MM-DD format to the source file 

'''
def add_date_field(file_contents, input_date, header_rec = True):
    # Convert Date value in YYYYMMDD format to YYYY-MM-DD format and prefix to file record
    try:
        lines = file_contents.split("\n")
        file_lines = []
        for index, line in enumerate(lines):
            if (index == 0 and header_rec == True and line.strip() != ''):
                file_lines.append("File_Date,"+line)
            if line.strip() != '' and (index != 0 and header_rec == True) :
                file_lines.append(datetime.strptime(input_date,'%Y%m%d').strftime('%Y-%m-%d')+","+line)

        file_contents = ('\n').join(file_lines)
        logging.info(
            "Content has been corrected to correct date field")
        return file_contents
    except:
        logging.info(
            "Content couldn't be fixed to correct date field")
        raise SystemExit()

''' 
remove_html() function removes the HTML content from the source file

'''
def remove_html(file_contents, header_rec = True):
    try:
        CLEANR = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')
        lines = file_contents.split("\n")
        file_lines = []
        for index, line in enumerate(lines):
            if (index == 0 and header_rec == True and line.strip() != ''):
                file_lines.append(line)
            if line.strip() != '' and (index != 0 and header_rec == True) :
                file_lines.append(re.sub(CLEANR, '', line))

        file_contents = ('\n').join(file_lines)
        logging.info(
            "Content has been corrected to remove html")
        return file_contents
    except:
        logging.info(
            "Content couldn't be fixed to remove html")
        raise SystemExit()


def copy_table_data(**kwargs):
    config_table_list = kwargs['config_table_list']
    for table_item in config_table_list['tablelist']:
        dataset = table_item['dataset']
        tablename = table_item['tablename']
        if table_exists(dataset, tablename) and table_exists(dataset + "_copy", tablename):
            stored_proc_query = "CALL edwhr_copy.copy_table_data('" + \
                dataset + "','" + tablename + "')"
            logging.info(stored_proc_query)
            df = pd.read_gbq(stored_proc_query, project_id=bq_project_id)
    return None


def getbqddl_file(bq_dataset, bq_table):
    logging.info("==== In getbqddl_file wit params: ====")
    logging.info("Dataset : {}, Table : {}".format(bq_dataset, bq_table))
    try:
        bqclient = bigquery.Client(bq_project_id)
        dataset_ref = bqclient.dataset(bq_dataset)
        table_ref = dataset_ref.table(bq_table)
        table = bqclient.get_table(table_ref)
        bqclient.schema_to_json(table.schema, bq_table + '.schema.json')
        table_schema_json_file = os.path.abspath(bq_table + '.schema.json')
        logging.info("Downloaded BQ Table Schema json File {}".format(
            table_schema_json_file))
        return table_schema_json_file
    except:
        logging.info(
            "Table {} could not be found for DDL Generation".format(bq_table))
        raise SystemExit()


def getbqddl(bq_dataset, bq_table):
    logging.info("==== In getbqddl with params: ====")
    logging.info("Dataset : {}, Table : {}".format(bq_dataset, bq_table))
    table_schema_json_file = getbqddl_file(bq_dataset, bq_table)
    with open(table_schema_json_file, 'r') as tsf:
        tblschema = json.load(tsf)
    logging.info("Downloaded BQ Table Schema json {}".format(tblschema))
    return tblschema


def table_exists(bq_dataset, bq_table):
    logging.info("==== In table_exists with params: ====")
    logging.info("Dataset : {}, Table : {}".format(bq_dataset, bq_table))
    try:
        bqclient = bigquery.Client(bq_project_id)
        dataset_ref = bqclient.dataset(bq_dataset)
        table_ref = dataset_ref.table(bq_table)
        table = bqclient.get_table(table_ref)
        if table:
            return True
    except NotFound:
        logging.info("Table {} does not exist in dataset {}".format(
            bq_table, bq_dataset))
        return False


def read_csv_into_pandas(srcfilename, delimiter, quote_char, encoding_scheme, table_schema_json):
    column_names = []
    for index in range(len(table_schema_json)):
        column_names.append(table_schema_json[index]['name'])

    df = pd.read_csv(srcfilename,
                     header=0,
                     #  skiprows=1,
                     delimiter=delimiter,
                     quotechar=quote_char,
                     quoting=1,
                     encoding=encoding_scheme,
                     #  names=column_names,
                     # need to test a callable function here
                     on_bad_lines='warn',
                     na_filter=False,
                     low_memory=False
                     )

    total_cols = len(df.axes[1])
    column_names = column_names[:total_cols]
    df.columns = column_names

    df = df.astype(str)
    df.columns = df.columns.str.lower()
    df_record_count = len(df.index)
    df["dw_last_update_date_time"] = datetime.now(
        current_timezone).replace(microsecond=0)
    logging.info("Row count in dataframe  is {}".format(df_record_count))


def upload_to_gcs(srclocalfilename, sourcesysname, tgt_folder, target_file_name, bq_project_id, gcs_bucket):
    logging.info(f"Uplodaing using args {bq_project_id} {gcs_bucket} {tgt_folder} {sourcesysname} {target_file_name} {srclocalfilename}")
    bucket = get_bucket(bq_project_id, gcs_bucket)
    blob = get_blob(bucket, tgt_folder, sourcesysname, target_file_name)
    blob.upload_from_filename(srclocalfilename)


def clean_add_delimiters_date(src_folder_path, srcfilename, sourcesysname, delimiter,header,  quote_char,quoting, escape_char,  encoding_scheme, add_date=True):
    print("got args")
    print(sourcesysname, src_folder_path, srcfilename, delimiter, quote_char, encoding_scheme, add_date)
    srcfilelocalname = downloadgcsfile(sourcesysname,srcfilename,encoding_scheme='UTF-8')

    df = pd.read_csv(srcfilelocalname, 
                    delimiter=delimiter, 
                    encoding='UTF-8')
    if add_date:
        df["dw_last_update_date_time"] = datetime.now().replace(microsecond=0)
    df = df.fillna('')
    df.to_csv(srcfilelocalname, index=False)
    upload_to_gcs(srcfilelocalname,sourcesysname,src_folder,srcfilename,bq_project_id,gcs_bucket)
    
    
def load_bq_table(bq_dataset, bq_table, dataframe, table_schema_json):
    # try:
    bqclient = bigquery.Client(bq_project_id)
    job_config = bigquery.job.LoadJobConfig()
    # table_id = f'{bq_project_id}.{bq_staging_dataset}.{tgt_bq_table}'
    tgt_bq_table = bq_dataset + '.' + bq_table
    if table_exists(bq_dataset, bq_table):
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    else:
        table = bigquery.Table(tgt_bq_table, schema=table_schema_json)
        bqclient.create_table(table)
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY
    job_config.schema = table_schema_json  # Schema from schema.json file
    job = bqclient.load_table_from_dataframe(
        dataframe, tgt_bq_table, job_config=job_config)
    # pandas_gbq.to_gbq(dataframe, tgt_bq_table,project_id=bq_project_id, if_exists='append', table_schema=getbqddl(tgt_bq_table))
    logging.info(
        'Completed loading the data into BigQuery. The table name is {}'.format(tgt_bq_table))

    tgt_record_count = pd.read_gbq(
        "select count(1) as count from " + tgt_bq_table, project_id=bq_project_id)
    tgt_record_count = list(tgt_record_count['count'])[0]
    logging.info("Row count in target bq table {} is {}".format(
        tgt_bq_table, tgt_record_count))

    return None
    # except:
    #     print("Could not load Table {}".format(tgt_bq_table))
    #     raise SystemExit()


def preprocess_file_for_staging(**kwargs):
    sourcesysname = kwargs['sourcesysname']
    source_file_name_prefix = kwargs['source_file_name_prefix']
    source_file_extension = kwargs['source_file_extension']
    tgt_bq_table = kwargs['tgt_bq_table']
    delimiter = kwargs['delimiter']
    quote_char = kwargs['quote_char']
    is_prefixed_file_name = kwargs['is_prefixed_file_name']
    encoding_scheme = kwargs['encoding_scheme']
    bq_staging_dataset = kwargs['bq_staging_dataset']
    preprocessing_steps = kwargs['preprocessing_steps']

    if is_prefixed_file_name == 'Yes':
        source_file_path = get_gcs_filepath(
            sourcesysname, source_file_name_prefix)
    else:
        source_file_path = '{}{}/{}.{}'.format(
            src_folder, sourcesysname, source_file_name_prefix, source_file_extension)
    srcfilename = source_file_path[source_file_path.rindex("/")+1:]

    if any(x in preprocessing_steps for x in ['decrypt_file']):
        srcfilelocalname = downloadgcsfile(sourcesysname, srcfilename, encoding_scheme=None)
        private_key = kwargs['private_key']
        decrypt_files(srcfilelocalname, private_key, sourcesysname, srcfilename)
        upload_object_from_file(sourcesysname, src_folder, srcfilename, srcfilelocalname)

    srcfilelocalname = downloadgcsfile(sourcesysname, srcfilename, encoding_scheme)
    search_steps = ['fix_nonescaped_characters','remove_multiple_headers']
    if any(x in preprocessing_steps for x in search_steps):
        file_contents = read_contents_file(
                    srcfilelocalname, encoding_scheme)
    
    for step in preprocessing_steps:
        if (step == 'fix_nonescaped_characters'):
            # match step:
            #     case 'fix_nonescaped_characters':
            file_contents = fix_nonescaped_character_contents(
                file_contents, delimiter, quote_char)
        elif (step == 'remove_multiple_headers'):
            file_contents = remove_multiple_headers(file_contents)
        elif (step == 'remove_footer'):
            file_contents = remove_footer(file_contents)
        elif (step.startswith('correct_date_field')):
            field_number = int(step.rsplit('_',1)[1])
            file_contents = correct_date_field(file_contents, field_number)
        elif (step == 'add_date_field'):
            input_date = datetime.now().strftime('%Y%m%d')
            file_contents = add_date_field(file_contents, input_date)
        elif (step == 'remove_html'):
            file_contents = remove_html(file_contents)
        elif (step == 'renaming'):
            # case 'renaming':
            # Change File Name to remove date time suffix
            removegcsfile(sourcesysname, src_folder, srcfilename)
            srcfilename = '{}.{}'.format(
                source_file_name_prefix, source_file_extension)

    if (len(preprocessing_steps)) > 0:
        if any(x in preprocessing_steps for x in search_steps):
            with open(srcfilelocalname, 'w', encoding=encoding_scheme) as file:
                file.seek(0)
                file.truncate()
                file.write(file_contents)
        upload_object_from_file(sourcesysname, src_folder,
                                srcfilename, srcfilelocalname)

    table_schema_json_file = getbqddl_file(bq_staging_dataset, tgt_bq_table)
    table_schema_json_file_name = table_schema_json_file[table_schema_json_file.rindex(
        "/")+1:]
    upload_object_from_file(sourcesysname, schema_folder,
                            table_schema_json_file_name, table_schema_json_file)

    return "Preprocessing is completed for file {}".format(srcfilename)


''' 
getschema() function gets the schema in JSON format from the BigQuery Table 
which inturn will be used while loading

'''
def getschema(sourcesysname,tgttablename):
    table_schema_json_file = getbqddl_file(stage_dataset, tgttablename)
    table_schema_json_file_name = table_schema_json_file[table_schema_json_file.rindex(
        "/")+1:]
    upload_object_from_file(sourcesysname, schema_folder,
                            table_schema_json_file_name, table_schema_json_file)
    return "Schema obtained for file {}".format(tgttablename)

''' 
get_count() function gets the count from the target table after being loaded

'''

def get_count(bq_staging_dataset,tgttablename):
    config={
        "job_type":"Query",
        "query":{
            "query":'select count(*) as total_count from ' + bq_staging_dataset+ '.' + tgttablename,
            "useLegacySql":False,
            "allow_large_results":True,
            "location":'US'
        }}
    hook = BigQueryHook(gcp_conn_id=bq_project_id,
     delegate_to=None, use_legacy_sql=False)
    target_count=hook.insert_job(configuration=config, project_id=bq_project_id, nowait=False)
    def set_default(obj):
        if isinstance(obj, set):
            return list(obj)
        raise TypeError
    for row in target_count.result():
        # print(f'PRINTING TOTAL_COUNT --> {row.total_count}')
        logging.info(
                "=================The {} table contains {} records=================".format(tgttablename, row.total_count))
        # return {row.total_count}
        result = json.dumps(row.total_count, default=set_default)
    return result

''' 
expected_count_from_src() function gets the count of records from the source file through the footer record

'''
def expected_count_from_src(src_folder, sourcesysname, source_file_name_prefix, source_file_extension,encoding_scheme):
    source_file_path = '{}{}/{}.{}'.format(src_folder, sourcesysname, source_file_name_prefix, source_file_extension)
    srcfilename = source_file_path[source_file_path.rindex("/")+1:]
    srcfilelocalname = downloadgcsfile(sourcesysname, srcfilename, encoding_scheme)
    contents = read_contents_file(srcfilelocalname, encoding_scheme)
    footer_list = contents.rsplit("\n", 1)

    if len(footer_list) == 1:
        footer = footer_list[0]
    else:
        footer = footer_list[1]
        if len(footer.strip()) == 0:
            footer_list = contents.rsplit("\n", 2)
            if len(footer_list) == 2:
                footer = footer_list[0] + "\n"
            else:
                footer = footer_list[1] + "\n"
    print(footer)
    footer_list=footer.split(",")
    expected_count=footer_list[1]
    print(expected_count)
    logging.info(
        "=================The {}.{} file contains {} records=================".format(source_file_name_prefix, source_file_extension,expected_count))    
    return expected_count

''' 
audit_table() adds an audit entry after the file has been loaded

'''

def audit_table(srcfileid,sourcesysname,srcfilename,tgttablename,src_rec_count,tgt_rec_count,tableload_start_time,dag_id):
            tableload_end_time = str(pendulum.now(current_timezone))[:23]
            tableload_run_time = (pd.to_datetime(
                tableload_end_time) - pd.to_datetime(tableload_start_time))

            if src_rec_count == tgt_rec_count:
                audit_status = 'PASS'
            else:
                audit_status = 'FAIL'

            # tgt_count = pd.read_gbq(
            #     "select count(1) as tgt_count from " + tgttablename, project_id=bqproject_id)
            # tgt_count = tgt_count['tgt_count'][0]
            audit_insert_stt = "insert into {} values (GENERATE_UUID() , {}, '{}', '{}', '{}', '{}',  {}, {}, '{}', '{}', '{}', '{}', '{}', '{}'  ) " .format(
                config['env']['v_audittablename'],
                srcfileid,
                sourcesysname,
                srcfilename,
                stage_dataset + "." + tgttablename,
                'RECORD_COUNT',
                src_rec_count,
                tgt_rec_count,
                tableload_start_time,
                tableload_end_time,
                tableload_run_time,
                dag_id + "-" + pendulum.now(current_timezone).strftime("%Y%m%d%H%M%S"),
                str(pendulum.now(current_timezone))[:23],
                audit_status

            )
            audit_entry = pd.read_gbq(
                audit_insert_stt,  project_id=bq_project_id, max_results=0)
            logging.info(
                "===Audit entry added for srcfileid {} - srcfilename {} ===".format(srcfileid, srcfilename))


def archive_file_and_cleanup(**kwargs):
    sourcesysname = kwargs['sourcesysname']
    source_file_name_prefix = kwargs['source_file_name_prefix']
    source_file_extension = kwargs['source_file_extension']
    tgt_bq_table = kwargs['tgt_bq_table']
    encoding_scheme = kwargs['encoding_scheme']

    srcfilename = '{}.{}'.format(
        source_file_name_prefix, source_file_extension)
    srcfilelocalname = downloadgcsfile(
        sourcesysname, srcfilename, encoding_scheme)
    archivegcsfile(sourcesysname, srcfilelocalname,
                   source_file_name_prefix, source_file_extension)
    removegcsfile(sourcesysname, src_folder, srcfilename)
    removelocalfile(tgt_bq_table)
    removelocalfile(srcfilelocalname)
    return "File {} has been archived and cleanup is completed".format(srcfilename)


def archive_srcfile(sourcesysname, srcfilename, encoding):
    # download file to local
    srcfilelocalname = downloadgcsfile(sourcesysname, srcfilename, encoding)
    # archive file
    archivegcsfile(sourcesysname, srcfilelocalname,
                   srcfilename.split('.')[0], srcfilename.split('.')[1])
    # Remove files
    removegcsfile(sourcesysname, src_folder, srcfilename)
    removelocalfile(srcfilelocalname)

def aggregate_hcapatientresponses(df):
# Post processing function for file Weekly_HCAPatientResponses
    print("===== In aggregate_hcapatientresponses ====")
    df.drop_duplicates(keep='first', inplace=True)
    logging.info("Row count in dataframe after removing dupliactes is {}".format(len(df.index)))
    return df

def pre_process(function,src_folder_path,srcfilename,sourcesysname,delimiter,header,quotechar,quoting,escape_char,encoding):
    if function:
        eval(function+"(src_folder_path,srcfilename,sourcesysname,delimiter,header,quotechar,quoting,escape_char,encoding)")

    print("===== In Pre-Process ====")
    pass

def crc32c(data):
    """
    Calculates the CRC32C checksum of the provided data.
    Args:
        data: the bytes over which the checksum should be calculated.
    Returns:
        An int representing the CRC32C checksum of the provided bytes.
    """
    import crcmod
    import six
    crc32c_fun = crcmod.predefined.mkPredefinedCrcFun('crc-32c')
    return crc32c_fun(six.ensure_binary(data))


def encrypt_symmetric(key_name, plaintext):
    """
    Encrypt plaintext using a symmetric key.
    Returns: Encrypted ciphertext.
    """

    from google.cloud import kms
    import base64
    plaintext_bytes = plaintext.encode('utf-8')
    plaintext_crc32c = crc32c(plaintext_bytes)
    kmsclient = kms.KeyManagementServiceClient()
    encrypt_response = kmsclient.encrypt(
        request={'name': key_name, 'plaintext': plaintext_bytes, 'plaintext_crc32c': plaintext_crc32c})

    # integrity verification
    if not encrypt_response.verified_plaintext_crc32c:
        raise Exception(
            'The request sent to the server was corrupted in-transit.')
    if not encrypt_response.ciphertext_crc32c == crc32c(encrypt_response.ciphertext):
        raise Exception(
            'The response received from the server was corrupted in-transit.')
    #return the cipherstring  without the b' '
    cipherstring = str(base64.b64encode(encrypt_response.ciphertext))[2:-1]
    return cipherstring


def decrypt_symmetric(key_name, ciphertext):
    """
    Decrypt the ciphertext using the symmetric key
    Returns: DecryptResponse: Response including plaintext.
    """

    from google.cloud import kms
    kmsclient = kms.KeyManagementServiceClient()
    ciphertext_crc32c = crc32c(ciphertext)
    decrypt_response = kmsclient.decrypt(request={
                                         'name': key_name, 'ciphertext': ciphertext, 'ciphertext_crc32c': ciphertext_crc32c})

    # integrity verification
    if not decrypt_response.plaintext_crc32c == crc32c(decrypt_response.plaintext):
        raise Exception(
            'The response received from the server was corrupted in-transit.')
    #return decrypted response plain text
    decrypttext = decrypt_response.plaintext
    return decrypttext


def access_secret(secret_resourceid):
    """
    Return the content of latest version of secret 
    for input resource
    """
    from google.cloud import secretmanager
    client = secretmanager.SecretManagerServiceClient()
    payload = client.access_secret_version(
        secret_resourceid).payload.data.decode("UTF-8")
    return payload


def sftp_get_files_old(sourcesysname, remote_directory,file_pattern,pwd_secret, host, user):
        logging.info("==== In sftp_get_files with params: ====")
        logging.info("Source System : {}, Remote Directory : {}, File Pattern : {}, Pwd Secret : {}, host: {}, User:{}".format(sourcesysname, remote_directory,file_pattern,pwd_secret, host, user))
        ssh_client = paramiko.SSHClient()
        ssh_client.load_system_host_keys()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        password=access_secret(pwd_secret)
        ssh_client.connect(hostname=host, port=22,username=user,password=password, banner_timeout=3600)
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
                for file in ftp_client.listdir(remote_directory):
                    is_pattern = re.search(pattern.get('name'), file)
                    if is_pattern:
                        print("File matching regex",file)
                        remotelocation = remote_directory+file
                        filelocation = '/home/airflow/gcs/data/'+file
                        print(filelocation)
                        ftp_client.get(remotelocation, filelocation)
                        file_line_count = int(subprocess.check_output(['wc','-l', filelocation]).split()[0])
                        # minus 1 to remove header from csv file to get actual records
                        file_line_count -= 1
                        print(f"File Name {filelocation}, Record count {file_line_count}")
                        file_details[src_name][file] = file_line_count
                        #file_details.append({src_name: {file: file_line_count}})
                        upload_object_from_file(sourcesysname, src_folder, file, filelocation)
                        removelocalfile(filelocation) 
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


def sftp_get_files(sourcesysname, remote_directory,file_pattern,pwd_secret, host, user):
        logging.info("==== In sftp_get_files with params: ====")
        logging.info("Source System : {}, Remote Directory : {}, File Pattern : {}, Pwd Secret : {}, host: {}, User:{}".format(sourcesysname, remote_directory,file_pattern,pwd_secret, host, user))
        ssh_client = paramiko.SSHClient()
        ssh_client.load_system_host_keys()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        password=access_secret(pwd_secret)
        ssh_client.connect(hostname=host, port=22,username=user,password=password, banner_timeout=3600)
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


def sftp_get_files_remove_date_marker(sourcesysname, remote_directory,file_pattern,pwd_secret, host, user, remove_date_marker, file_prefix, file_extension):
        logging.info("==== In sftp_get_files with params: ====")
        logging.info("Source System : {}, Remote Directory : {}, File Pattern : {}, Pwd Secret : {}, host: {}, User:{}".format(sourcesysname, remote_directory,file_pattern,pwd_secret, host, user))
        ssh_client = paramiko.SSHClient()
        ssh_client.load_system_host_keys()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        password=access_secret(pwd_secret)
        ssh_client.connect(hostname=host, port=22,username=user,password=password, banner_timeout=3600)
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
                print(pattern)
                src_name = re.sub(r"[^a-zA-Z0-9_]+", '', pattern.get('pattern').split('.')[-2])
                file_details[src_name] = {}                
                for file_name in ftp_client.listdir(remote_directory):
                    is_pattern = re.search(pattern.get('pattern'), file_name)
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
                        print("Remove Date Marker ",remove_date_marker)
                        if remove_date_marker == 'Yes':
                            file_name = f"{file_prefix}.{file_extension}"
                        print("File Name : ", file_name)
                        upload_object_from_file(sourcesysname, src_folder, file_name, filelocation)
                        removelocalfile(filelocation)
                        if pattern.get('pattern') not in patterns_found:
                            patterns_found.append(pattern.get('pattern'))                        
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


def sftp_delete_files(sourcesysname, remote_directory,file_pattern,pwd_secret, host, user):
        logging.info("==== In sftp_delete_files with params: ====")
        logging.info("Source System : {}, Remote Directory : {}, File Pattern : {}, Pwd Secret : {}, host: {}, User:{}".format(sourcesysname, remote_directory,file_pattern,pwd_secret, host, user))
        ssh_client = paramiko.SSHClient()
        ssh_client.load_system_host_keys()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        password=access_secret(pwd_secret)
        ssh_client.connect(hostname=host, port=22,username=user,password=password, banner_timeout=3600)
        print("connection successful")
        #open sftp
        ftp_client=ssh_client.open_sftp()
        print("SFTP opened")
        file_details = {}
        try:
            listing = ftp_client.listdir()
            print("Directory List",listing)
            print("Files in Directory",ftp_client.listdir(remote_directory))
            file_details = {}
            load_start_time = pendulum.now('US/Central').to_datetime_string()
            patterns_found = []
            for pattern in file_pattern:
                src_name = re.sub(r"[^a-zA-Z0-9_]+", '', pattern.get('pattern').split('.')[-2])
                file_details[src_name] = {}                
                for file_name in ftp_client.listdir(remote_directory):
                    is_pattern = re.search(pattern.get('pattern'), file_name)
                    if is_pattern:
                        print("File matching regex",file_name)
                        remotelocation = remote_directory+file_name
                        ftp_client.remove(remotelocation)                    
        except Exception as e:
            print("error",e)
            raise AirflowFailException
        ftp_client.close()
        print("Connection Closed!")
        return file_details, load_start_time


def update_audit_data(**kwargs):
    """
    This method is used to update audit data into audit_control table using data gathers in xcom from other functions 
    such as sftp_get_file etc. and club that with stage table counts and insert records into audit_control table
    """
    src_config = kwargs["src_config"]
    bq_client = bigquery.Client(project=config['env']['v_curated_project_id'])    
    print("Start_Audit_Data")
    ti = kwargs['ti']
    audit_file_data, load_start_time = ti.xcom_pull(task_ids='sftp_get_files')
    load_end_time = pendulum.now('US/Central')
    print(audit_file_data)
    dependency = kwargs['dependency']
    table_counts = {}
    src_tgt_mapping = {}
    for idx,item in enumerate(dependency):
        source = item["source"].get("name")
        target = item["target"].get("name")  
        src_name = re.sub(r"[^a-zA-Z0-9_]+", '',source.split('.')[-2]) 
        target_table = config['env']['v_curated_project_id'] +'.' + config['env']['v_hr_stage_dataset_name'] + '.' + target         
        result = bq_client.query(f'select count(*) as ct from {target_table}').result()
        for i in result:
            table_counts[src_name] =  i["ct"]
            src_tgt_mapping[src_name] = target
    audit_records = []
    for src_id, src in enumerate(audit_file_data):
        expected_value = str(sum(list(map(int,list(audit_file_data[src].values())))))
        actual_value = str(table_counts.get(src,0))
        if expected_value == actual_value:
            audit_status = 'PASS'
        elif actual_value > expected_value:
            audit_status = 'PASS(More records in Target)'
        else:
            audit_status = 'FAIL'
        audit_records.append({'uuid' : str(uuid.uuid4()),
                      'table_id': src_id+1,#src_tgt_mapping[src],
                      'src_sys_nm': src_config['ingest'][0]['source_system'],
                      'src_tbl_nm': src,
                      'tgt_tbl_nm': stage_dataset + '.' + src_tgt_mapping[src],
                      'audit_type': 'RECORD_COUNT',
                      'expected_value':expected_value,
                      'actual_value': actual_value,
                      'load_start_time': load_start_time,
                      'load_end_time': load_end_time.to_datetime_string(),
                      'load_run_time': str(datetime.fromisoformat(load_end_time.to_datetime_string()) - datetime.fromisoformat(load_start_time)),
                      #'load_run_time': (load_end_time - pendulum.parse(load_start_time)).in_seconds(),
                      'job_name': kwargs['ti'].dag_id + '-' + pendulum.now(timezone).strftime("%Y%m%d%H%M%S"),
                      'audit_time': str(pendulum.now(timezone))[:23],
                      'audit_status': audit_status })

    print(f"audit records are {audit_records}")
    audit_table = bq_client.dataset(config['env']['v_hr_audit_dataset_name']).table(config['env']['v_audittablename'].split('.')[-1])
    table1 = bq_client.get_table(audit_table)
    audit_result = bq_client.insert_rows(table1,audit_records)
    print(f"result of inserting into audit is {audit_result}")


def insert_audit_data(**kwargs):
    """
    This method is used to insert audit data into audit_control table by getting the record count for the source file and staging table
    """
    print("Start_Audit_Data")
    sourcesysname = kwargs['sourcesysname']
    dag_id = kwargs['dag_id']
    source_id = kwargs['source_id']
    source_file_name_prefix = kwargs['source_file_name_prefix']
    source_file_extension = kwargs['source_file_extension']
    encoding_scheme = kwargs['encoding_scheme']
    tolerance_percent = kwargs['tolerance_percent']
    source_file_name = f'{source_file_name_prefix}.{source_file_extension}'
    bucket = get_bucket(bq_project_id, gcs_bucket)
    blob = get_blob(bucket, src_folder, sourcesysname, source_file_name)
    blob.download_to_filename(source_file_name)
    expected_value = sum(1 for _ in enumerate(open(source_file_name, encoding=encoding_scheme)))
    expected_value = expected_value - 1
    print("Expected Count : ", expected_value)

    bq_staging_dataset = kwargs['bq_staging_dataset']
    target = kwargs['tgt_bq_table']
	
    load_start_time = kwargs['load_start_time']
    load_end_time = pendulum.now('US/Central')
    
    bq_client = bigquery.Client(project=config['env']['v_curated_project_id'])
    target_table = f'{bq_staging_dataset}.{target}'        
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
    audit_table = bq_client.dataset(config['env']['v_hr_audit_dataset_name']).table(config['env']['v_audittablename'].split('.')[-1])
    audit_table_ref = bq_client.get_table(audit_table)
    audit_result = bq_client.insert_rows(audit_table_ref,audit_records)
    print(f"result of inserting into audit is {audit_result}")

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
        bucket = get_bucket(config['env']['v_curated_project_id'], config['env']['v_dag_bucket_name'])
        storage_client = storage.Client()
        source = kwargs["source"].lower()
        validationsql_folder = f"dags/sql/validation_sql/integrate/{source}"
        bq_table = kwargs["bq_table"].lower()
        blobs = storage_client.list_blobs(
            bucket, prefix=validationsql_folder, delimiter=None)
        file_list = [a for a in blobs
                      if a.name.endswith(bq_table + ".sql")]
        client = bigquery.Client(project = config['env']['v_curated_project_id'])
        if file_list:
            for filename in file_list:
                bqsqlsqryfile = open(base_dir + filename.name.replace('dags',''))
                bqsqlsqry = bqsqlsqryfile.read()
                replacements = {
                    '{{ params.param_hr_stage_dataset_name }}': config['env']['v_curated_project_id'] +
                                  '.' + config['env']['v_hr_stage_dataset_name'],
                    '{{ params.param_hr_core_dataset_name }}': config['env']['v_curated_project_id'] +
                                  '.' + config['env']['v_hr_core_dataset_name'],
                    '{{ params.param_hr_base_views_dataset_name }}': config['env']['v_curated_project_id'] +
                                  '.' + config['env']['v_hr_base_views_dataset_name'],
                    '{{ params.param_hr_views_dataset_name }}': config['env']['v_curated_project_id'] +
                                  '.' + config['env']['v_hr_views_dataset_name'],
                    '{{ params.param_dim_core_dataset_name }}': config['env']['v_curated_project_id'] +
                                  '.' + config['env']['v_dim_core_dataset_name'],
                    '{{ params.param_dim_base_views_dataset_name }}': config['env']['v_curated_project_id'] +
                                  '.' + config['env']['v_dim_base_views_dataset_name'],
                    '{{ params.param_pub_views_dataset_name }}': config['env']['v_curated_project_id'] +
                                  '.' + config['env']['v_pub_views_dataset_name'],
                    '{{ params.param_hr_audit_dataset_name }}': config['env']['v_curated_project_id'] +
                                  '.' + config['env']['v_hr_audit_dataset_name']
                }

                # Loop through the dictionary and replace each key with its corresponding value
                for old_text, new_text in replacements.items():
                    bqsqlsqry = bqsqlsqry.replace(old_text, new_text)

                logging.info("===Executing SQL : {}===".format(filename))
                logging.info(bqsqlsqry)

                job_config = bigquery.QueryJobConfig(
                         query_parameters=[
                            bigquery.ScalarQueryParameter("p_tableload_start_time", "DATETIME", start_date),
                            bigquery.ScalarQueryParameter("p_tableload_end_time", "DATETIME", end_date),
                            bigquery.ScalarQueryParameter("p_job_name", "STRING", kwargs["task_id"]),
                            bigquery.ScalarQueryParameter("p_source", "STRING", source),
                            bigquery.ScalarQueryParameter("p_table", "STRING", bq_table),
                        ]
                        )
                query_job = client.query(bqsqlsqry, job_config=job_config)
                logging.info(query_job)
        else:
            logging.info(
                "===Did not find any Validation SQL for table - {}===".format(bq_table))
            

def executevalidationsqls_ext(**kwargs):
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
        bucket = get_bucket(config['env']['v_curated_project_id'], config['env']['v_dag_bucket_name'])
        storage_client = storage.Client()
        source = kwargs["source"].lower()
        validationsql_folder = f"dags/sql/validation_sql/integrate/{source}"
        bq_table = kwargs["bq_table"].lower()
        blobs = storage_client.list_blobs(
            bucket, prefix=validationsql_folder, delimiter=None)
        file_list = [a for a in blobs
                      if a.name.endswith('j_hdw_'+bq_table + ".sql")]
        client = bigquery.Client(project = config['env']['v_curated_project_id'])
        if file_list:
            for filename in file_list:
                bqsqlsqryfile = open(base_dir + filename.name.replace('dags',''))
                bqsqlsqry = bqsqlsqryfile.read()
                replacements = {
                    '{{ params.param_hr_stage_dataset_name }}': config['env']['v_curated_project_id'] +
                                  '.' + config['env']['v_hr_stage_dataset_name'],
                    '{{ params.param_hr_core_dataset_name }}': config['env']['v_curated_project_id'] +
                                  '.' + config['env']['v_hr_core_dataset_name'],
                    '{{ params.param_hr_base_views_dataset_name }}': config['env']['v_curated_project_id'] +
                                  '.' + config['env']['v_hr_base_views_dataset_name'],
                    '{{ params.param_hr_views_dataset_name }}': config['env']['v_curated_project_id'] +
                                  '.' + config['env']['v_hr_views_dataset_name'],
                    '{{ params.param_dim_core_dataset_name }}': config['env']['v_curated_project_id'] +
                                  '.' + config['env']['v_dim_core_dataset_name'],
                    '{{ params.param_dim_base_views_dataset_name }}': config['env']['v_curated_project_id'] +
                                  '.' + config['env']['v_dim_base_views_dataset_name'],
                    '{{ params.param_pub_views_dataset_name }}': config['env']['v_curated_project_id'] +
                                  '.' + config['env']['v_pub_views_dataset_name'],
                    '{{ params.param_hr_audit_dataset_name }}': config['env']['v_curated_project_id'] +
                                  '.' + config['env']['v_hr_audit_dataset_name']
                }

                # Loop through the dictionary and replace each key with its corresponding value
                for old_text, new_text in replacements.items():
                    bqsqlsqry = bqsqlsqry.replace(old_text, new_text)

                logging.info("===Executing SQL : {}===".format(filename))
                logging.info(bqsqlsqry)

                job_config = bigquery.QueryJobConfig(
                         query_parameters=[
                            bigquery.ScalarQueryParameter("p_tableload_start_time", "DATETIME", start_date),
                            bigquery.ScalarQueryParameter("p_tableload_end_time", "DATETIME", end_date),
                            bigquery.ScalarQueryParameter("p_job_name", "STRING", kwargs["task_id"]),
                            bigquery.ScalarQueryParameter("p_source", "STRING", source),
                            bigquery.ScalarQueryParameter("p_table", "STRING", bq_table),
                        ]
                        )
                query_job = client.query(bqsqlsqry, job_config=job_config)
                logging.info(query_job)
        else:
            logging.info(
                "===Did not find any Validation SQL for table - {}===".format(bq_table))
            
#
def decrypt_files(srcfilelocalname, private_key, sourcesysname, srcfilename):
    """
    This method is used to decrypt 
    """
    encoding_scheme = None
    private_key_blob = access_secret(private_key)
    key_private, _ = pgpy.PGPKey.from_blob(private_key_blob)
    pgp_file = pgpy.PGPMessage.from_file(srcfilelocalname)
    decrypted_data = key_private.decrypt(pgp_file).message
    toread = io.BytesIO()
    toread.write(bytes(decrypted_data))
    toread.seek(0)
    with open(srcfilelocalname, 'wb') as f:
        shutil.copyfileobj(toread, f)
        f.close()
    upload_object_from_file(sourcesysname, src_folder, srcfilename, srcfilelocalname)
    return

def remove_columns_from_csv(input_bucket, src_folder_path, input_file, output_bucket):

    client = storage.Client()
    input_path = f"gs://{input_bucket}/{src_folder_path}/{input_file}"
    output_path = f"gs://{output_bucket}/{src_folder_path}/{input_file}"
    preferred_encodings = ["utf-8", "iso-8859-1", "cp1252","Windows-1252"]
    for encoding_page in preferred_encodings:
        print(encoding_page)
        try:
           df = pd.read_csv(input_path,encoding=encoding_page)
           if len(df.columns) > 26:
            df = df.iloc[:, :-2]
            df.to_csv(output_path, index=False)
            print(f"removed last 2 columns from file")
            break

           else:     
            print(f"file does not have more than 26 columns")
        except UnicodeDecodeError as e:
           print(f"UnicodeDecodeError with encoding {encoding_page} : {e} ")
        continue
""""
def remove_columns_from_csv(input_bucket, src_folder_path, input_file, output_bucket):

    client = storage.Client()
    input_path = f"gs://{input_bucket}/{src_folder_path}/{input_file}"
    output_path = f"gs://{output_bucket}/{src_folder_path}/{input_file}"
    df = pd.read_csv(input_path)
    if len(df.columns) > 26:
            df = df.iloc[:, :-2]
            df.to_csv(output_path, index=False)
            print(f"removed last 2 columns from file")
    else:
        print(f"file does not have more than 26 columns")

"""
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
    expected_value = expected_value-1
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
    audit_table = bq_client.dataset(kwargs['config']['env']['v_hr_audit_dataset_name']).table(kwargs['config']['env']['v_audittablename'].split('.')[-1])
    audit_table_ref = bq_client.get_table(audit_table)
    audit_result = bq_client.insert_rows(audit_table_ref,audit_records)
    print(f"result of inserting into audit is {audit_result}")