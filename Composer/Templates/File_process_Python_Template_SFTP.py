import logging
import os
import sys
import traceback
import csv

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage
from io import StringIO

import yaml
import json
import time
import pendulum
import argparse
import string
import random
import re

timezone = pendulum.timezone("US/Central")

cwd = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.dirname(cwd)
sys.path.insert(0, base_dir)
utilpath = base_dir + '/utils/'
config_folder = base_dir + "/config/"

def call_config_yaml(filename):
    # load input config .yaml file from config folder
    cfgfile = open(config_folder + filename)
    default_config = yaml.safe_load(cfgfile)
    default_config = json.loads(json.dumps(default_config, default=str))
    config = default_config
    return config

global config
config = call_config_yaml("sc_config.yaml")


class process_files(beam.DoFn):
    
    def __init__(self, config, sourcesysnm, srcsys_config):
        self.config = config
        self.source_system = sourcesysnm
        self.srcsys_config = srcsys_config
    
    def list_blobs(self, file_name_pattern): 
    # Main Funcation called from dag to match file pattern and merge files if required
    # Check if Regex pattern is passed
        import re
        
       
        logging.info("==== In listblobswithpattern with params====")
        storage_client = storage.Client(self.config['env']['v_landing_project_id'])
        bucket = storage_client.bucket(self.config['env']['v_data_bucket_name'])
        
        src_folder_prefix = f"{self.config['env']['v_srcfilesdir']}{self.source_system}"
        logging.info(f"==== Source folder for files: {src_folder_prefix} ====")
        blobs = storage_client.list_blobs(
            bucket, prefix=src_folder_prefix, delimiter=None)
        blob_list = []
        
        if '$' in file_name_pattern:
            # Find file/s matching pattern
            for blob in blobs:
                is_pattern = re.search(file_name_pattern, re.sub(r"[^a-zA-Z0-9_]+", '', blob.name))
            
                if is_pattern:
                    logging.info("File matching regex {} in bucket {} : {}".format(
                        file_name_pattern, self.config['env']['v_data_bucket_name'], blob.name))
                    blob_list.append(blob.name)

        else:
            
            file_name_pattern_gcs = re.sub(r"[^a-zA-Z0-9_.]+", '', file_name_pattern.replace('*','')[:-1])
            logging.info(f"file name pattern GCS: {file_name_pattern_gcs}")
            for blob in blobs:
                logging.info('Comparing for filename: {} with pattern: {}'.format(blob.name,file_name_pattern))
                basename = os.path.basename(blob.name)
                logging.info(f"Base Name: {basename}")  
                if basename.startswith(file_name_pattern_gcs):
                    blob_list.append(blob.name)    

                
                # if os.path.basename(blob.name) == file_name_pattern:
                #     blob_list.append(blob.name)
                #     logging.info('Filename found: {}'.format(blob.name))

        # Merge file if merge is requested
        if len(blob_list) > 0:
            return blob_list
        else:
            logging.info("Source File Path is not found for file prefix {} in bucket {}".format(
                file_name_pattern, self.config['env']['v_data_bucket_name']))

    def archive_files(self, file_list):
        
        storage_client = storage.Client(self.config['env']['v_landing_project_id'])
        bucket = storage_client.bucket(self.config['env']['v_data_bucket_name'])
        
        try:
            for file in file_list:

                source_blob = bucket.blob(file)
                target_path = file.replace(self.config['env']['v_srcfilesdir'], self.config['env']['v_archivedir'])
                blob_copy = bucket.copy_blob(
                    source_blob, bucket, target_path
                )
                # Remove the source blob
                source_blob.delete()
        
        except:
            logging.error(f"Archival process failed")
            logging.error(traceback.format_exc())
            raise SystemExit()
    
    def del_gcs_files(self, file_list):
        
        storage_client = storage.Client(self.config['env']['v_landing_project_id'])
        bucket = storage_client.bucket(self.config['env']['v_data_bucket_name'])
        
        try:
            for file in file_list:

                source_blob = bucket.blob(file)
                
                source_blob.delete()
        
        except:
            logging.error(f"Deleting of GCS files failed")
            logging.error(traceback.format_exc())
            raise SystemExit()

    def unzip_remove_non_ascii_chars(self, file_name, replace_non_ascii_chars, source_format):
        
        import os
        import gzip
        from tempfile import NamedTemporaryFile
        from google.cloud import storage
        import zipfile
        
        storage_client = storage.Client(self.config['env']['v_landing_project_id'])
        bucket = storage_client.bucket(self.config['env']['v_data_bucket_name'])

        logging.info(f"==== Preprocessing for file {file_name} has started ====")
        unzipped_file_name = file_name.replace(f"{self.config['env']['v_srcfilesdir']}",
                                                           f"{self.config['env']['v_stagefilesdir']}")
        zipped_file_blob = bucket.blob(file_name)
        unzipped_file_blob = bucket.blob(unzipped_file_name)
        file_ext = file_name.split('.')[-1]
        
        try:
            
            # Remove non ASCII chars from the legacy file
            # removed_chars_ascii = list(range(0,10)) + [11,12] + list(range(14,32)) + list(range(128,256))
            removed_chars_ascii = [0]
            removed_chars_ascii_dict = {c:' ' for c in removed_chars_ascii}
            
            temp_txt = NamedTemporaryFile(delete=False)
            temp_json = NamedTemporaryFile(delete=False)
            
            # Unzip the data file and write to a temp file as in memory is taking longer and Pardo times out
            zipped_file = zipped_file_blob.open(mode='rb')

            if file_ext == 'zip':
                # unzip as zip file
                with zipfile.ZipFile(zipped_file, 'r') as zip:
                    with open(temp_txt.name, 'w+t') as unzip:

                        logging.info(f"zipsk: {zip}")
                        logging.info(f"ziplistsk: {zip.namelist()}")

                        content = zip.read(zip.namelist()[0]) # 100 MB
        
                        content = content.decode('utf-8', errors='replace')
                        if replace_non_ascii_chars  == True:    
                            content = content.translate(removed_chars_ascii_dict)

                        unzip.write(content)

                        unzip.seek(0)   
                        
                        logging.info(f"==== File {file_name} has been unzipped successfully ====")

            else:
                #unzip as gun-zip
                with gzip.open(zipped_file, 'r') as zip:
                    with open(temp_txt.name, 'w+t') as unzip:
                        
                        iteration_cnt=0
                        while True:
                            content=zip.read(size=104857600) # 100 MB
                            content = content.decode('cp1252')
                            
                            if replace_non_ascii_chars  == True:    
                                content = content.translate(removed_chars_ascii_dict)
                            
                            unzip.write(content)
                            
                            iteration_cnt+=1
                            logging.info(f"==== Iteration - {iteration_cnt} for {file_name} completed ====")
                            
                            if not content:
                                break

                        unzip.seek(0)   
                        
                        logging.info(f"==== File {file_name} has been unzipped successfully ====")


            zipped_count = 0
            if source_format != 'CSV':
                # Count the data and convert the file into JSONNL format
                zipped_count = 0
                with open(temp_json.name, 'w+t') as unzip_json:
                    with open(temp_txt.name, 'r') as unzip_txt:
                        for zipped_count, d in enumerate(unzip_txt):
                            json.dump({'data':d.rstrip("\n"),'file_name':file_name},unzip_json)
                            unzip_json.write("\n")

                            if zipped_count % 100000 == 0:
                                logging.info(f"==== File {unzipped_file_name} - {zipped_count} rows processed to JSONNL format")
                
                zipped_count += 1

                logging.info(f"==== File {unzipped_file_name} has {zipped_count} rows ====")
                
                # Upload the JSONNL format of the file to GCS
                unzipped_file_blob.upload_from_filename(temp_json.name)
                logging.info(f"==== File {file_name} converted to JSON and uploaded to GCS successfully ====")

            else:
                
                unzipped_file_blob.upload_from_filename(temp_txt.name)
                logging.info(f"==== File {file_name} uploaded to GCS successfully ====")

            # Remove the temp files
            temp_txt.close()
            os.remove(temp_txt.name)
            temp_json.close()
            os.remove(temp_json.name)

            logging.info(f"==== Local copy of the file {file_name} removed ====")
            logging.info(f"==== Preprocessing for file {file_name} has completed ====")

            return zipped_count, unzipped_file_name, file_name

        except:
            logging.error(f"Unzip operation failed for file {file_name}")
            logging.error(traceback.format_exc())
            raise SystemExit()
    
    def gcs_bq_load(self, file_list, table_name, write_disposition, delimiter, quote_character, skip_leading_rows, allow_jagged_rows, source_format="CSV"):
        
        from google.cloud import bigquery
        import time
        
        bigquery_client = bigquery.Client(self.config['env']['v_curated_project_id'])
        
        logging.info(f"==== Loading of files {file_list} from GCS to BQ has started ====")
        
        if write_disposition.lower() == 'append':
            write_disposition = 'WRITE_APPEND'
        elif write_disposition.lower() == 'truncate':
            write_disposition = 'WRITE_TRUNCATE'
        
        table_name = table_name.replace('v_stage_dataset_name', config['env']['v_curated_project_id'] + 
                                    '.' + config['env']['v_sc_stage_dataset_name'])
        logging.info(f"Original file_list: {file_list}")

        file_list = [f"gs://{self.config['env']['v_data_bucket_name']}/{x}" for x in file_list]
        
        logging.info(f"Updated file_list: {file_list}")
        
        if source_format == 'NEWLINE_DELIMITED_JSON':
            job_config = bigquery.LoadJobConfig(
                write_disposition=write_disposition,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            )
        else:
            job_config = bigquery.LoadJobConfig(
                write_disposition=write_disposition,
                field_delimiter=delimiter,
                autodetect=False,
                source_format=bigquery.SourceFormat.CSV,
                quote_character=quote_character,
                skip_leading_rows=skip_leading_rows,
                allow_jagged_rows=allow_jagged_rows,
                allow_quoted_newlines = True
            )

        # Loading data from GCS to BigQuery Stage Tables 
        load_job = bigquery_client.load_table_from_uri(
            source_uris=file_list,
            destination=table_name,
            job_config=job_config
        )
        job_output = bigquery_client.get_job(load_job.job_id)

        # Waits for job to complete 
        while job_output.state != "DONE":
            time.sleep(5)
            job_output = bigquery_client.get_job(load_job.job_id)

        if job_output.state == "DONE":
            try:
                load_job.result()
                logging.info(f"==== Loading of file {file_list} from GCS to BQ has completed ====")
            except Exception as err:
                logging.error(f"==== Loading of file {file_list} from GCS to BQ has failed with error message - {err} ====")
                logging.error(traceback.format_exc())
                raise SystemExit()
                
    def count_records_in_csv(self, file_name):
        storage_client = storage.Client(self.config['env']['v_landing_project_id'])
        bucket = storage_client.bucket(self.config['env']['v_data_bucket_name'])
        blob = bucket.blob(file_name)

        content = blob.download_as_text()
        csv_file = StringIO(content)

        csv_reader = csv.reader(csv_file)
        record_count = sum(1 for _ in csv_reader)

        return record_count
        
    def insert_audit_entry(self, file_list, table_name, tableload_start_time, job_name):
        import pandas as pd
        import pandas_gbq
        record_count = 0

        for file_name in file_list:
            record_count = self.count_records_in_csv(file_name)

            #record_count += record_count

        file_list_str = ','.join(file_list)

        logging.info(f"==== Audit entry for files {file_list_str} has started ====")

        tableload_end_time = str(pendulum.now(timezone).in_timezone(timezone))[:26]
        tableload_run_time = (pd.to_datetime(tableload_end_time) - pd.to_datetime(tableload_start_time))
        table_name = table_name.replace('v_stage_dataset_name', config['env']['v_curated_project_id'] + 
                                    '.' + config['env']['v_sc_stage_dataset_name'])
        # Query to count records in the table
        count_query = f"SELECT COUNT(*) as table_count FROM `{table_name}`"
        table_count_result = pandas_gbq.read_gbq(count_query, project_id=config['env']['v_curated_project_id'], dialect='standard')

        # Extracting the actual table count from the result
        actual_table_count = table_count_result.iloc[0]['table_count']

        # Set audit status based on whether the actual count matches the expected count
        audit_status = 'PASS' if actual_table_count == record_count else 'FAIL'
            
        audit_query = f"""INSERT INTO {config['env']['v_audittablename']} 
            (uuid,src_sys_nm,src_tbl_nm,tgt_tbl_nm,audit_type,expected_value,actual_value,load_start_time,load_end_time,load_run_time,job_name,audit_time,audit_status)
            VALUES (
                GENERATE_UUID(),
                '{self.source_system}',
                '{file_list_str}', 
                '{table_name}', 
                'RECORD_COUNT', 
                {record_count}, 
                {actual_table_count}, 
                '{tableload_start_time}',
                '{tableload_end_time}',
                '{tableload_run_time}',
                '{job_name}',
                datetime_trunc(current_datetime('US/Central'), SECOND),
                '{audit_status}')"""
                    
        print(audit_query)
        pandas_gbq.read_gbq(audit_query, project_id=config['env']['v_curated_project_id'], max_results=0)

        logging.info(f"==== Audit entry for file {file_list_str} has completed ====")

    def insert_audit_entry_json_nl(self, file_data_count, table_name, tableload_start_time, job_name):
        
        import pandas as pd
        import pandas_gbq

        for file_count_dict in file_data_count:
            
            exp_count = file_count_dict["count"]
            file_name = file_count_dict["src_file_name"]
            
            logging.info(f"==== Audit entry for file {file_name} has started ====")

            tableload_end_time = str(pendulum.now(timezone).in_timezone(timezone))[:26]
            tableload_run_time = (pd.to_datetime(tableload_end_time) - pd.to_datetime(tableload_start_time))
            table_name = table_name.replace('v_stage_dataset_name', config['env']['v_curated_project_id'] + 
                                        '.' + config['env']['v_sc_stage_dataset_name'])
            # Query to count records in the table
            count_query = f"SELECT COUNT(*) as table_count FROM `{table_name}` where file_name = '{file_name}'"
            table_count_result = pandas_gbq.read_gbq(count_query, project_id=config['env']['v_curated_project_id'], dialect='standard')

            # Extracting the actual table count from the result
            actual_table_count = table_count_result.iloc[0]['table_count']

            # Set audit status based on whether the actual count matches the expected count
            audit_status = 'PASS' if actual_table_count == exp_count else 'FAIL'
            
            audit_query = f"""INSERT INTO {config['env']['v_audittablename']} 
                (uuid,src_sys_nm,src_tbl_nm,tgt_tbl_nm,audit_type,expected_value,actual_value,load_start_time,load_end_time,load_run_time,job_name,audit_time,audit_status)
                VALUES (
                    GENERATE_UUID(),
                    '{self.source_system}',
                    '{file_name}', 
                    '{table_name}', 
                    'RECORD_COUNT', 
                    {exp_count}, 
                    {actual_table_count}, 
                    '{tableload_start_time}',
                    '{tableload_end_time}',
                    '{tableload_run_time}',
                    '{job_name}',
                    datetime_trunc(current_datetime('US/Central'), SECOND),
                    '{audit_status}')"""
                        
            pandas_gbq.read_gbq(audit_query, project_id=config['env']['v_curated_project_id'], max_results=0)

            logging.info(f"==== Audit entry for file {file_name} has completed ====")

    def process(self, element):
        
        import pendulum

        tableload_start_time = str(pendulum.now(timezone).in_timezone(timezone))[:26]
        file_list = self.list_blobs(element)
       
        logging.info(f"Blobs listed - {file_list}")
        logging.info(f"Element: {element}")
        logging.info(f"Blobs listed - {file_list}")
        logging.info(f"source system config - {srcsys_config}")       
        
        # table_info = [ x for x in srcsys_config["filelist"]]
        table_info = [x for x in srcsys_config["filelist"] if any(re.sub(r"[^a-zA-Z0-9_.*]+", '', name) == re.sub(r"[^a-zA-Z0-9_.*]+", '', ''.join(element)) for name in x["source"]["name"])]          
        
        logging.info(f"table info - {table_info}")

        # for info in table_info:

        #     table_name = info["target"].get("name", None)
        #     logging.info(f"table name - {table_name}")            
        #     write_disposition = info["source"].get("write_disposition", "truncate")
        #     delimiter = info["source"].get("delimiter",",")
        #     quote_character = info["source"].get("quote_character","")
        #     skip_leading_rows = info["source"].get("skip_leading_rows",0)
        #     zip_file = info["source"].get("zip_file",False)

        #     print("zip_file",zip_file)
        #     replace_non_ascii_chars = info["source"].get("replace_non_ascii_chars",False)
        #     allow_jagged_rows = info["source"].get("allow_jagged_rows",False)

        table_name = table_info[0]["target"].get("name", None)
        write_disposition = table_info[0]["source"].get("write_disposition", "truncate")
        delimiter = table_info[0]["source"].get("delimiter",",")
        quote_character = table_info[0]["source"].get("quote_character","")
        skip_leading_rows = table_info[0]["source"].get("skip_leading_rows",0)
        zip_file = table_info[0]["source"].get("zip_file",False)

        print("zip_file",zip_file)
        replace_non_ascii_chars = table_info[0]["source"].get("replace_non_ascii_chars",False)
        allow_jagged_rows = table_info[0]["source"].get("allow_jagged_rows",False)        

        if zip_file:
            source_format = table_info[0]["source"].get("source_format","NEWLINE_DELIMITED_JSON")
            unzip_file_list = []
            data_count = []
            for file in file_list:
                count, unzipped_file, src_file_name = self.unzip_remove_non_ascii_chars(file, replace_non_ascii_chars,source_format)
                unzip_file_list.append(unzipped_file)
                logging.info(f"unzipped_file_list:{unzip_file_list}")
                data_count.append({"count":count, "src_file_name":src_file_name})
             
            self.gcs_bq_load(unzip_file_list, table_name, write_disposition, delimiter, quote_character, skip_leading_rows, allow_jagged_rows, source_format)
            
            print(data_count)
            if source_format == 'CSV':
                self.insert_audit_entry(unzip_file_list, table_name, tableload_start_time, jobname)
            else:
                self.insert_audit_entry_json_nl(data_count, table_name, tableload_start_time, jobname)

            logging.info("Archival started")
            self.archive_files(file_list)
            self.del_gcs_files(unzip_file_list)
            logging.info("Archival completed")

        else:
            
            source_format = "CSV"
            self.gcs_bq_load(file_list, table_name, write_disposition, delimiter, quote_character, skip_leading_rows, allow_jagged_rows, source_format)
            
            self.insert_audit_entry(file_list, table_name, tableload_start_time, jobname)

            logging.info("Archival started")
            self.archive_files(file_list)
            logging.info("Archival completed")
        
def run():
 
    parser = argparse.ArgumentParser()
    args, beam_args = parser.parse_known_args()

    beam_options = PipelineOptions(
        beam_args,
        project = config['env']['v_proc_project_id'],
        service_account_email = config['env']['v_serviceaccountemail'],
        job_name = jobname,
        runner = config['env']['v_runner'],
        network = config['env']["v_network"],
        subnetwork = config['env']["v_subnetwork"],
        staging_location = config['env']["v_dfstagebucket"],
        temp_location = config['env']["v_gcs_temp_bucket"],
        region = config['env']["v_region"],
        save_main_session = True,
        num_workers = config['env']["v_numworkers"],
        max_num_workers = config['env']["v_maxworkers"],
        use_public_ips = False,
        # dataflow_service_options = ["enable_prime"],
        autoscaling_algorithm = "THROUGHPUT_BASED",
        worker_machine_type = srcsys_config['v_machine_type'],        
        setup_file = '{}setup.py'.format(utilpath),
        # experiments = ["no_use_multiple_sdk_containers"],
        disk_size_gb = 250)

    try:
        logging.info("===Apache Beam Run with pipeline options started===")
        
        pcoll = beam.Pipeline(options=beam_options)
        pcoll | "Initialize file list" >> beam.Create(src_file_list) | "Process source files" >> beam.ParDo(
            process_files(config, sourcesysnm, srcsys_config ))

        logging.info("===Submitting Asynchronous Dataflow Job===")
        
        p = pcoll.run()
        
        if config['env']['v_runner'] == 'DataflowRunner':
            dataflow_job_id = p.job_id()
            logging.info( "===Submitted Asynchronous Dataflow Job, Job id is " + dataflow_job_id + " ====")
            return dataflow_job_id

    except:
        logging.exception("===ERROR: Apache Beam Run Failed===")
        raise SystemExit()

def main(sourcesysnm, src_file_list):

    # Generate unique jobname
    global jobname
    randomstring = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
    jobname = sourcesysnm.replace("_","-") + "-p-" + time.strftime("%Y%m%d%H%M%S") + '-' + randomstring
    logging.info("===Job Name is {} ===".format(jobname))
    
    # Run the dataflow job
    p1 = run()
    dataflow_job_id = p1

    logging.info("===END: Data Pipeline for src_file_list {}-{} at {}===".format(
        sourcesysnm, src_file_list, time.strftime("%Y%m%d-%H:%M:%S")))
    print(dataflow_job_id)
    return dataflow_job_id

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--src_sys_config_file", required=True,
                        help=("Source System Based Config file name"))
    parser.add_argument("--src_file_list", required=True,
                        help=("Source File List"))
                     
    args = parser.parse_args()

    srcsys_config_file = args.src_sys_config_file

    global srcsys_config
    srcsys_config = call_config_yaml(args.src_sys_config_file)
    srcsys_config = srcsys_config

    src_file_list = [args.src_file_list]

    sourcesysnm = srcsys_config["v_sourcesysnm"]
    sourcesysnm = sourcesysnm.replace("-","_")

    logging.info("===BEGIN: Data Pipeline for src_file_list {}-{} at {}===".format(
        sourcesysnm, src_file_list, time.strftime("%Y%m%d-%H:%M:%S")))
    main(sourcesysnm, src_file_list)