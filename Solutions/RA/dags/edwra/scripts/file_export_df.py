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
import pendulum
import yaml
import json
import argparse
import string
import random
import time
import logging
import pandas as pd
from datetime import datetime

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

class setuprunnerenv(beam.DoFn):
    def process(self, context):
        # use /tmp/ on dataflow worker node for processing
        global base_dir
        base_dir = '/tmp/'
        os.system('java -version')
        return list("1")

class WriteToSFTP(beam.DoFn):
    def process(self, element, bq_source, server_dest, ra_config_param):
        logging.info("=== In Process ===")
        logging.info("=== In Start ===")
        import paramiko
        ssh_client = paramiko.SSHClient()
        ssh_client.load_system_host_keys()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        name = str(ra_config_param['env']["v_pwd_secrets_url"]) + str(ra_config_param['env']["v_secret_name"]) + "/versions/latest"
        from google.cloud import secretmanager
        client = secretmanager.SecretManagerServiceClient()
        response = client.access_secret_version(name=name)
        payload = response.payload.data.decode('UTF-8')
        logging.info("===Grabbing secrets===")

        logging.info("===Attempting to connect===")
        ssh_client.connect(hostname=ra_config_param['env']["v_linux_server"], port = 22, username='td_ra', password=payload, timeout=3600) 
        logging.info("===Connected===")
        sftp = ssh_client.open_sftp()
        logging.info("===opened SFTP===")

        for source, dest in zip(bq_source, server_dest):

            if source == "done_file": 
                done_file = 'empty.done'
                open(done_file, 'w').close()

                # Adding datetime extension
                file_name,extension = os.path.splitext(dest)
                logging.info(file_name)
                logging.info(extension)
                today = pendulum.now('US/Central').date()
                formatted_date = today.strftime('%Y%m%d')
                updated_dest = "{}_{}{}".format(file_name, formatted_date, extension)

                remote_path = ra_config_param['env']["v_linux_file_path_donefile"] + updated_dest

                # Delete old done files
                logging.info(f"Deleting old done files with prefix {file_name}")
                delete_command = f"find {ra_config_param['env']['v_linux_file_path_donefile']} -type f -name '{file_name}*' -exec rm -f {{}} +"
                stdin, stdout, stderr = ssh_client.exec_command(delete_command)
                errors = stderr.read().decode()
                if errors:
                    logging.error(f"Error encountered while deleting done files with prefix {file_name}")
                    logging.error(errors)
                else:
                    logging.info(f"Successfully deleted any files with prefix {file_name}")

                sftp.put(done_file, remote_path)
                logging.info(f"==== done file with name {updated_dest} uploaded to server ====")
                remote_command = f"chmod 777 {remote_path}"
                stdin, stdout, stderr = ssh_client.exec_command(remote_command)
                errors = stderr.read().decode()
                if errors:
                    logging.error(f"===Error when changing permissions for file {updated_dest} ===")
                    logging.error(errors)
                else:
                    logging.info(f"===File Permissions Changed for {updated_dest}===")
            else: 
                logging.info("=== Exporting File to Server ===")
                from google.cloud import storage
                storage_client = storage.Client()
                bucket = storage_client.bucket(ra_config_param['env']["v_gcs_outboundfile_bucket_name"])
                
                
                blob = bucket.blob(ra_config_param['env']["v_gcs_outboundfile_folder"] + source)
                local_path = '/tmp/'

                file_name,extension = os.path.splitext(dest)
                if file_name[-3:].upper() == '_BQ':
                    updated_dest = "{}{}".format(file_name, extension)
                else:
                    updated_dest = "{}{}{}".format(file_name, ra_config_param['env']["v_bq_suffix"].upper(), extension)

                remote_path = ra_config_param['env']["v_linux_file_path"] + updated_dest
                blob.download_to_filename(local_path + source)
                sftp.put(local_path + source, remote_path, confirm=False)
                logging.info("==== file uploaded to server ====")

                remote_command = f"chmod 777 {remote_path}"
                stdin, stdout, stderr = ssh_client.exec_command(remote_command)
                errors = stderr.read().decode()
                if errors:
                    logging.error("===Error when changing permissions ===")
                    logging.error(errors)
                else:
                    logging.info("===File Permissions Changed===")
            
        logging.info("===Closing Connection===")
        sftp.close()
        ssh_client.close()
        logging.info("===Closed Connection===")
        return


def run():
    pipeline_args = [
        "--project", config['env']['v_proc_project_id'],
        "--service_account_email", config['env']['v_df_atos_serviceaccountemail'],
        "--job_name", jobname,
        "--runner", config['env']['v_runner'],
        "--network", config['env']["v_network"],
        "--subnetwork", config['env']["v_subnetwork"],
        "--staging_location", config['env']["v_dfstagebucket"],
        "--temp_location", config['env']["v_gcs_temp_bucket"],
        "--region", config['env']["v_region"],
        "--save_main_session",
        "--num_workers", str(config['env']["v_numworkers"]),
        "--max_num_workers", str(config['env']["v_maxworkers"]),
        "--no_use_public_ips",
        "--dataflow_service_options, enable_prime",
        "--autoscaling_algorithm", "THROUGHPUT_BASED",
        "--worker_machine_type", config['env']["v_worker_machine_type"],
        "--setup_file", '{}setup.py'.format(utilpath)
    ]

    logging.info("===Apache Beam Run with pipeline options started===")
    pcoll = beam.Pipeline(argv=pipeline_args)
    pcoll | "Initialize" >> beam.Create(["1"]) | "Setup Dataflow Worker">> beam.ParDo(setuprunnerenv()) | 'Write to SFTP' >> beam.ParDo(WriteToSFTP(), bq_source = source_list, server_dest = dest_list, ra_config_param = ra_config)
    logging.info("===Submitting Asynchronous Dataflow Job===")
    p = pcoll.run()
    dataflow_job_id = p.job_id()
    print(dataflow_job_id)
    return dataflow_job_id


def main():
    global jobname
    randomstring = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))

    jobname = "file-extract" + '-' + time.strftime("%Y%m%d%H%M%S")
    logging.info("===Job Name is {} ===".format(jobname))
    p1 = run()
    dataflow_job_id = p1

    logging.info("===END: Data Pipeline for job {} at {}===".format((jobname), time.strftime("%Y%m%d-%H:%M:%S")))
    print(dataflow_job_id)
    return dataflow_job_id


if __name__ == "__main__":
    project_id = config['env']['v_proc_project_id']
    os.environ['GOOGLE_CLOUD_PROJECT'] = project_id

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