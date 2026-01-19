import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging
import yaml
import sys
import time
import pendulum
import os
import argparse
from datetime import datetime
import math
import pyarrow
import logging
from google.cloud import secretmanager
from google.cloud import bigquery

timezone_str = "US/Central"
timezone = pendulum.timezone(timezone_str)

def call_config_yaml(filepath):
    with open(filepath, 'r') as cfgfile:
        config = yaml.safe_load(cfgfile)
    return config

def access_secret(secret_resourceid: str) -> str:
    '''Retrieve and decode password from GCP Secret Manager based on secret name'''
    client = secretmanager.SecretManagerServiceClient()
    payload = client.access_secret_version(name=secret_resourceid).payload.data.decode("UTF-8")
    return payload

class ReadFromSourceDB(beam.DoFn):
    def __init__(self, config,s,h,u,password,source_schema,date_fields):
        self.config = config
        self.s =s
        self.h = h
        self.u = u
        self.password = password
        self.source_schema = source_schema
        self.date_fields = date_fields

    def process(self, partition):
            def transform_columns(row):
                # print(row)
                for idx, col in enumerate(self.source_schema):
                    if col in self.date_fields:
                        raw_date_val = row[idx]
                        # print(raw_date_val)
                        if raw_date_val is None:
                            transformed_date_val = None
                        else:
                            transformed_date_val = str(raw_date_val)[:10]
                        row[idx] = transformed_date_val
                    if row[idx] is None:
                        row[idx] = ''
                    if '|' in str(row[idx]):
                        row[idx] = '"' + row[idx] + '"'
                    elif '"' in str(row[idx]):
                        row[idx] = str(row[idx]).replace('"', '""')
                        row[idx] = '"' + row[idx] + '"'
                    if isinstance(row[idx], str): 
                        # Replace newlines with a placeholder  
                        row[idx] = row[idx].replace("\n", " \\n ").replace("\r\n", " \\n ")
                        # Replace tab characters with a single space
                        row[idx] = row[idx].replace("\t", " ")
                        # Ensure there are no leading newline characters that might cause unwanted row breaks
                        row[idx] = row[idx].lstrip("\n").lstrip("\r")
                        # Ensure that the row ends with no extra newline at the end (if any)
                        row[idx] = row[idx].rstrip("\n").rstrip("\r")
                # Join the row with the delimiter '|'
                row_str = '|'.join(str(ele) for ele in row)
                #return row_str
                # Ensure the entire row is treated as a single line (no unintended line breaks)
                return row_str.replace("\n", " \\n ").replace("\r", " ")
                #row_str = '|'.join(str(ele) for ele in row)
                #return row_str
            
            import oracledb

            query = self.config['query']
            partition_column = self.config['partition_column']
            num_partitions = self.config['num_partitions']
            chunksize = self.config.get('chunksize', 100000)
            formatted_clause = f"AND MOD(ABS({partition_column}),{num_partitions}) = {partition}"
            
            if num_partitions > 1:
                modified_query = f"{query} {formatted_clause}"
            else:
                modified_query = query
            logging.info(f"Running Query {modified_query}")
            conn_str='{}/{}@{}/{}'.format(self.u,self.password,self.h,self.s)
            try: 
                with oracledb.connect(conn_str) as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(modified_query)
                        while True:
                            chunk = cursor.fetchmany(chunksize)
                            if not chunk:
                                break
                            logging.info("fetched chunk")
                            yield '\n'.join([transform_columns(list(x)) for x in chunk])
            except Exception as e:
                logging.error(f"An error occurred: {e}")
                raise
            finally:
                logging.info("Connection safely closed.")

def get_table_schema(client, dataset_id, table_id):
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)  # API request
    return [{'name': field.name, 'type': field.field_type, 'mode': field.mode} for field in table.schema]

def run_pipeline(env_config_file, src_sys_config_file, full_pull_table, schema_id):
    cwd = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(cwd)
    sys.path.insert(0, base_dir)
    config_folder = os.path.join(base_dir, "config")
    utilpath = os.path.join(base_dir, "utils")
    sys.path.append(utilpath)
    logging.info(f'Util path is {utilpath}')
    import common_utilities as cu
    
    full_pull_table = full_pull_table.lower()
    srcsys_config = call_config_yaml(
        os.path.join(config_folder, src_sys_config_file))
    env_config = call_config_yaml(os.path.join(config_folder, env_config_file))
    max_num_workers = srcsys_config[full_pull_table]['max_workers']
    if schema_id == '1':
        s = srcsys_config['service_name_p1']
        h = srcsys_config['host_name']
        u = srcsys_config['user']
        secret_name = srcsys_config['secret_name_p1']
        password = access_secret(env_config['env']['v_pwd_secrets_url'] + secret_name)
        schema_instance = 'p1'
    elif schema_id == '3':
        s = srcsys_config['service_name_p2']
        h = srcsys_config['host_name']
        u = srcsys_config['user']
        secret_name = srcsys_config['secret_name_p2']
        password = access_secret(env_config['env']['v_pwd_secrets_url'] + secret_name)
        schema_instance = 'p2'
        max_num_workers = math.ceil(max_num_workers/2)
    client = bigquery.Client(project=env_config['env']['v_curated_project_id'])

    pipeline_options = PipelineOptions(
        runner='DataflowRunner',
        project=env_config['env']['v_proc_project_id'],
        job_name=f"ra-oracle-to-gcs-daily-{schema_instance}-{full_pull_table.replace('_','-')}-{time.strftime('%Y%m%d%H%M%S')}",
        temp_location=env_config['env']["v_gcs_temp_bucket"],
        region=env_config['env']["v_region"],
        setup_file=os.path.join(utilpath, 'setup.py'),
        service_account_email=env_config['env']["v_df_atos_serviceaccountemail"],
        network=env_config['env']["v_network"],
        subnetwork=env_config['env']["v_subnetwork"],
        staging_location=env_config['env']["v_dfstagebucket"],
        worker_machine_type=srcsys_config['machine_type'],
        autoscaling_algorithm="THROUGHPUT_BASED",
        num_workers=env_config['env']["v_numworkers"],
        max_num_workers=max_num_workers,
        use_public_ips=False
    )
    p = beam.Pipeline(options=pipeline_options)

    for table_config in srcsys_config[full_pull_table]['table_info']:
        # Set config
        target_schema = get_table_schema(client, table_config['dataset_id'], table_config['table_id_no_cdc'])
        date_fields = [col['name'] for col in target_schema if col['type'] == 'DATE']
        source_schema = table_config['source_schema']
        header = '|'.join(col for col in source_schema)
        output_path = f"{env_config['env']['v_gcs_temp_bucket']}/edwradata/{table_config['table_id_no_cdc']}/{schema_instance}"
        tableload_start_time = str(pendulum.now(timezone))[:19]
        table_config['query'] = table_config['query'].replace('v_curr_timestamp', tableload_start_time)
        table_config['query'] = table_config['query'].replace('$SchemaID', schema_id)
        table_config['query'] = cu.replace_edw_etl_load_field(table_config['query'], table_config['table_id_no_cdc'], schema_id)

        cu.oracle_preprocess_update(table_config['table_id'], schema_id)
        
        # Start pipeline
        partitions = p | f"Create {table_config['num_partitions']} Partitions for {table_config['table_id_no_cdc']}" >> beam.Create(range(table_config['num_partitions']))
        read_and_write = partitions | f"Read from SourceDB for {table_config['table_id_no_cdc']}" >> beam.ParDo(ReadFromSourceDB(table_config,s,h,u,password,source_schema,date_fields))
        read_and_write | f"Write to GCS for {table_config['table_id_no_cdc']}"  >> beam.io.WriteToText(output_path, header=header)
    
    logging.info("===Submitting Asynchronous Dataflow Job===")
    df_job = p.run()
    logging.info("=======DONE========")
    logging.info(f"Job ID is {df_job.job_id()}")
    # DO NOT REMOVE print: Need for xcom
    print(df_job.job_id())
    return df_job.job_id()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("-src", "--env_config_file", required=False, default="ra_config.yaml",
                        help=("Environment Config file name"))
    parser.add_argument("-c", "--src_sys_config_file", required=False, default="ra_oracle_ingest_dependency_daily_gcs.yaml",
                        help=("Source System Based Config file name"))
    parser.add_argument("-table", "--full_pull_table", required=False, default="",
                        help=("Table To Load"))
    parser.add_argument("-si", "--schema_id", required=True, default="",
                        help=("Source System Schema"))
    args = parser.parse_args()
    run_pipeline(args.env_config_file, args.src_sys_config_file,
                 args.full_pull_table, args.schema_id)