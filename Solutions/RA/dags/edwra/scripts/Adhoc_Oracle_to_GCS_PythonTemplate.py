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

def escape_special_characters(value):
    """
    Escapes special characters to ensure CLOB data is correctly inserted or written to output.
    """
    if not isinstance(value, str):
        return value
        value = value.replace("|", "\\|")  # Escape pipe characters inside the data
        value = value.replace("\n", " \\n ")  # Replace newlines with placeholder
        value = value.replace("\r\n", " \\n ")  # Handle Windows line breaks
        value = value.replace("\t", " ")  # Replace tabs with a space
        value = value.replace("\\", "\\\\")  # Escape backslashes
        value = value.replace('"', '""')  # Escape double quotes in CSV/TSV format
        value = value.strip("\n").strip("\r")  # Strip leading/trailing newlines
    return value

class ReadFromSourceDB(beam.DoFn):
    def __init__(self, config, s, h, u, password, source_schema, date_fields):
        self.config = config
        self.s = s
        self.h = h
        self.u = u
        self.password = password
        self.source_schema = source_schema
        self.date_fields = date_fields

    def escape_special_characters(self, value):
        """Escapes special characters to ensure CLOB data is correctly inserted or written to output.
    """
        if not isinstance(value, str):
            return value
        value = value.replace("|", "\\|")  # Escape pipe characters inside the data
        value = value.replace("\n", " \\n ")  # Replace newlines with placeholder
        value = value.replace("\r\n", " \\n ")  # Handle Windows line breaks
        value = value.replace("\r", " \\n ")  # Handle Windows line breaks
        value = value.replace("\t", " ")  # Replace tabs with a space
        value = value.replace("\\", "\\\\")  # Escape backslashes
        value = value.replace('"', '""')  # Escape double quotes in CSV/TSV format
        value = value.strip("\n").strip("\r")  # Strip leading/trailing newlines
        return f'"{value}"'

    def process(self, partition):
        def transform_columns(row):
            for idx, col in enumerate(self.source_schema):
                if col in self.date_fields:
                    raw_date_val = row[idx]
                    if raw_date_val is None:
                        transformed_date_val = None
                    else:
                        transformed_date_val = str(raw_date_val)[:10]
                    row[idx] = transformed_date_val
                
                if row[idx] is None:
                    row[idx] = ''
                # Escape CLOB or string data fields
                if isinstance(row[idx], str):
                    row[idx] = self.escape_special_characters(row[idx])
                
            # Join the row with the delimiter '|'
            row_str = '|'.join(str(ele) for ele in row)
            return row_str
        
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
        conn_str = '{}/{}@{}/{}'.format(self.u, self.password, self.h, self.s)
        
        try:
            with oracledb.connect(conn_str) as conn:
                def output_type_handler(cursor, name, defaultType, size, precision, scale):
                    if defaultType == oracledb.DB_TYPE_CLOB:
                        return cursor.var(oracledb.DB_TYPE_LONG, arraysize=cursor.arraysize)
                    return None

                with conn.cursor() as cursor:
                    cursor.outputtypehandler = output_type_handler
                    cursor.execute(modified_query)
                    while True:
                        chunk = cursor.fetchmany(chunksize)
                        if not chunk:
                            break
                        logging.info("Fetched chunk")
                        # Apply the transformation to each row
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

def run_pipeline(env_config_file, src_sys_config_file, adhoc_pull_table, schema_id):
    cwd = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(cwd)
    sys.path.insert(0, base_dir)
    config_folder = os.path.join(base_dir, "config")
    utilpath = os.path.join(base_dir, "utils")
    sys.path.append(utilpath)
    logging.info(f'Util path is {utilpath}')
    import common_utilities as cu
    
    adhoc_pull_table = adhoc_pull_table.lower()
    srcsys_config = call_config_yaml(
        os.path.join(config_folder, src_sys_config_file))
    env_config = call_config_yaml(os.path.join(config_folder, env_config_file))
    max_num_workers = srcsys_config[adhoc_pull_table]['max_workers']
    
    if schema_id == '1':
        s = srcsys_config['servicename_p1']
        h = srcsys_config['hostname']
        u = srcsys_config['v_user']
        secret_name = srcsys_config['v_pwd_secret_name_p1']
        password = access_secret(env_config['env']['v_pwd_secrets_url'] + secret_name)
        schema_instance = 'p1'
    elif schema_id == '3':
        s = srcsys_config['servicename_p2']
        h = srcsys_config['hostname']
        u = srcsys_config['v_user']
        secret_name = srcsys_config['v_pwd_secret_name_p2']
        password = access_secret(env_config['env']['v_pwd_secrets_url'] + secret_name)
        schema_instance = 'p2'
        max_num_workers = math.ceil(max_num_workers/2)
    
    client = bigquery.Client(project=env_config['env']['v_curated_project_id'])

    pipeline_options = PipelineOptions(
        runner='DataflowRunner',
        project=env_config['env']['v_proc_project_id'],
        job_name=f"ra-oracle-to-gcs-adhoc-{schema_instance}-{adhoc_pull_table.replace('_','-')}-{time.strftime('%Y%m%d%H%M%S')}",
        temp_location=env_config['env']["v_gcs_temp_bucket"],
        region=env_config['env']["v_region"],
        setup_file=os.path.join(utilpath, 'setup.py'),
        service_account_email=env_config['env']["v_df_atos_serviceaccountemail"],
        network=env_config['env']["v_network"],
        subnetwork=env_config['env']["v_subnetwork"],
        staging_location=env_config['env']["v_dfstagebucket"],
        worker_machine_type=srcsys_config['v_machine_type'],
        autoscaling_algorithm="THROUGHPUT_BASED",
        num_workers=env_config['env']["v_numworkers"],
        max_num_workers=max_num_workers,
        use_public_ips=False
    )
    p = beam.Pipeline(options=pipeline_options)

    for table_config in srcsys_config[adhoc_pull_table]['table_info']:
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
        read_and_write = partitions | f"Read from SourceDB for {table_config['table_id_no_cdc']}" >> beam.ParDo(ReadFromSourceDB(table_config, s, h, u, password, source_schema, date_fields))
        read_and_write | f"Write to GCS for {table_config['table_id_no_cdc']}"  >> beam.io.WriteToText(output_path, header=header)
    
    logging.info("===Submitting Asynchronous Dataflow Job===")
    df_job = p.run()
    logging.info("=======DONE========")
    logging.info(f"Job ID is {df_job.job_id()}")
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
    parser.add_argument("-c", "--src_sys_config_file", required=False, default="ra_oracle_ingest_dependency_adhoc.yaml",
                        help=("Source System Based Config file name"))
    parser.add_argument("-table", "--adhoc_pull_table", required=False, default="",
                        help=("Table To Load"))
    parser.add_argument("-si", "--schema_id", required=True, default="",
                        help=("Source System Schema"))
    args = parser.parse_args()
    run_pipeline(args.env_config_file, args.src_sys_config_file,
                 args.adhoc_pull_table, args.schema_id)
