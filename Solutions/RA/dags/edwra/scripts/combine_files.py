import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
from google.cloud import storage
from io import BytesIO
from io import StringIO
import paramiko
from paramiko import SSHClient
from google.cloud import secretmanager
import csv
import os
import sys
import yaml
import json
import argparse
import string
import random
import time
import logging
import pandas as pd
from datetime import datetime
from google.api_core.exceptions import TooManyRequests
import math

cwd = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.dirname(cwd)
sys.path.insert(0, base_dir)
utilpath = base_dir + '/utils/'
# script_dir = os.path.dirname(__file__)
# util_dir = os.path.join(script_dir, '..', 'utils')
config_folder = base_dir + "/config/"

def call_config_yaml(filename, variablename):
    # load input config .yaml file from config folder
    cfgfile = open(config_folder + filename)
    default_config = yaml.safe_load(cfgfile)
    default_config = json.loads(json.dumps(default_config, default=str))
    config = default_config
    return config

config = call_config_yaml("ra_config.yaml", "hca_ra_default_vars")
project_id = config['env']['v_proc_project_id']
storage_client = storage.Client(project_id)
local_file_path = './tmp.csv'

def download_blob(bucket_name, source_blob_name, filename):
    blob = storage_client.bucket(bucket_name).blob(source_blob_name)
    blob.download_to_filename(filename)

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)


def replace_quotes(file_path):
    with open(file_path, 'r') as file:
        file_data = file.read()

    file_data = file_data.replace('"""', '"')

    with open(file_path, 'w') as file:
        file.write(file_data)
def main():

    for source, dest in zip(source_list, dest_list):
        if source == "done_file": 
            continue 
        ## Adding code to check for chunked files 
        bucket_name = config['env']["v_gcs_outboundfile_bucket_name"]
        bucket = storage_client.bucket(bucket_name)
        
        prefix_file = source.rstrip('.txt')
        folder_path = config['env']["v_gcs_outboundfile_folder"]
        blobs = bucket.list_blobs(prefix=f'{folder_path}{prefix_file}')
        blobs_list = list(blobs)
        logging.info(f'{len(blobs_list)} chunked files were found to be combined')
        max_elements = 30
        # Breaks list into chunks < 32 (Limit for compose function is 32)
        blob_list_chunks = [blobs_list[i * max_elements:(i + 1) * max_elements] for i in range((len(blobs_list) + max_elements - 1) // max_elements )]
        destination = bucket.blob(f'{folder_path}{source}')
        if len(blobs_list) == 1: 
            pass
        else: 
            destination.upload_from_string('')
            for idx, blob_chunk in enumerate(blob_list_chunks):
                logging.info(idx)
                logging.info(len(blob_chunk))
                destination_blob = bucket.get_blob(f'{folder_path}{source}')
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

        logging.info(f'Downloading file {destination.name} to local')
        download_blob(bucket_name, destination.name, local_file_path)
        logging.info('Removing extra quotation characters')
        replace_quotes(local_file_path)
        logging.info(f'Reuploading file {destination.name}')
        upload_blob(bucket_name, local_file_path, destination.name)
        logging.info(f'Finished uploading file {destination.name}')

        if len(blobs_list)  > 1: 
            for blob in blobs_list:
                blob.delete()
            logging.info('Completed delete of chunked files')

if __name__ == "__main__":
    os.environ['GOOGLE_CLOUD_PROJECT'] = project_id
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('--source_files', nargs='*')
    parser.add_argument('--dest_files', nargs='*')
    parser.add_argument("--src_sys_config_file", required=True, help=("Source System Based Config file name"))

    args = parser.parse_args()

    global srcsys_config_file
    srcsys_config_file = args.src_sys_config_file

    global ra_config
    ra_config = call_config_yaml(args.src_sys_config_file, "hca_ra_default_vars")

    global source_list
    source_list = args.source_files

    global dest_list
    dest_list = args.dest_files

    main()