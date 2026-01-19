import logging
import os
import decimal
import traceback
from google.cloud import bigquery
import apache_beam as beam
from apache_beam.coders import TupleCoder, FastPrimitivesCoder
# from apache_beam.io.gcp.bigquery import ReadFromBigQuery
# import GroupIntoBatches
import sys
import pandas as pd
from pandas.errors import OutOfBoundsDatetime
import yaml
import json
import concurrent.futures
from google.cloud import secretmanager
import argparse
import string
import random
from datetime import datetime as dt
import time
import pendulum
import copy
import io
import numpy as np

timezone_str ="US/Central"
timezone = pendulum.timezone(timezone_str)

cwd = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.dirname(cwd)
sys.path.insert(0, base_dir)
utilpath = base_dir + '/utils/'
config_folder = base_dir +"/config/"


def call_config_yaml(filename, variablename):
    # load input config .yaml file from config folder
    cfgfile = open(config_folder + filename)
    default_config = yaml.safe_load(cfgfile)
    default_config = json.loads(json.dumps(default_config, default=str))
    config = default_config
    return config


config = call_config_yaml("ra_config.yaml","hca_ra_default_vars")
#config = call_config_yaml("ra_config.yaml","hca_ra_default_vars")

def access_secret(secret_resourceid: str) -> str:
    '''Retrieve and decode password from GCP Secret Manager based on secret name'''
    client = secretmanager.SecretManagerServiceClient()
    payload = client.access_secret_version(name=secret_resourceid).payload.data.decode("UTF-8")
    return payload

class setuprunnerenv(beam.DoFn):
    def process(self, context):

        # use /tmp/ on dataflow worker node for processing
        global base_dir
        base_dir = '/tmp/'
        # os.system('gsutil cp ' + gcsjarbucket +
        #           jdbcjar + ' ' + base_dir + ' && ls ')

        # # Copy required java libraries
        # os.system('gsutil cp ' + gcsjarbucket +
        #           jdkfile + ' ' + base_dir + ' && ls ')

        # # setup jvm path and java version
        # os.system('mkdir -p /usr/lib/jvm')
        # os.system('tar xvzf ' + base_dir + jdkfile + ' -C /usr/lib/jvm')
        # os.system(
        #     'update-alternatives --install"/usr/bin/java""java""/usr/lib/jvm/' + jdkversion + '/bin/java" 1 ')
        # os.system('update-alternatives --config java')
        # logging.info('JDK Libraries copied to Instance..')

        os.system('java -version')

        return list("1")
    
class TruncateOracleTables(beam.DoFn):
## calls stored procedure in oracle to truncate target table

    def __init__(self, conn_str):
        self.conn_str = conn_str
    def process(self, element):
        import oracledb

        oracle_tbl_name = f'{tbl_name}'.upper()
        logging.info(f'Truncating {oracle_tbl_name} in Oracle')
        if tbl_name == 'ACCT_EXT_DATA_STAGE':
            procedure_name = f'CONCUITY.TRUNC_EXT_DATA'
        elif tbl_name == 'ACCT_PYR_EXT_DATA_STAGE':
            procedure_name = f'CONCUITY.TRUNC_PYR_EXT_DATA'

        self.connection = oracledb.connect(self.conn_str)
        self.cursor = self.connection.cursor()
        self.cursor.callproc(procedure_name)

        logging.info(f'{oracle_tbl_name} Truncated in Oracle')

        self.cursor.close()
        self.connection.close()

class TransformToTuple(beam.DoFn):
## converts data to tuples to prep for load to oracle
    logging.info('Converting elements to tuples for insertion')
    def process(self, element):
        
        # logging.info(element)
        values = []
        if element:
            for col in tblschema:
                col_name = col['name']
                values.append(element.get(col_name))
            yield tuple(values)

class WriteToOracle(beam.DoFn):
    def __init__(self, conn_str):
        self.conn_str = conn_str
        logging.info('Beginning write to oracle')


    def start_bundle(self):
        import oracledb

        inputsizes = []
        for col in tblschema:
            if 'id' in col['name']:
                inputsizes.append(oracledb.DB_TYPE_NUMBER)
            if "dt" in col['name']:
                inputsizes.append(oracledb.DB_TYPE_DATE)
            elif "num" in col['name']:
                inputsizes.append(oracledb.DB_TYPE_NUMBER)
            elif "char" in col['name']:
                inputsizes.append(oracledb.STRING)
            elif col['name'] == "creation_dt":
                inputsizes.append(oracledb.DB_TYPE_DATE)
            elif col['name'] == "creation_user":
                inputsizes.append(oracledb.STRING)


        self.connection = oracledb.connect(self.conn_str)
        self.cursor = self.connection.cursor()
        self.cursor.setinputsizes(*inputsizes)
        logging.info('Beginning write to oracle')

    
    def process(self, elements):
        # remove schema ending from tbl_name
        oracle_tbl_name = f'{tbl_name}'.upper()
        try:
            insert_part = ", ".join(col['name'].upper() for col in tblschema)
            values_part = ", ".join(f":{i+1}" for i in range(len(tblschema)))

            oracle_query = f"INSERT INTO CONCUITY.{oracle_tbl_name} ({insert_part}) VALUES ({values_part})"
            self.cursor.executemany(oracle_query, elements)
            self.connection.commit()
            logging.info(f"{len(elements)} loaded to Concuity table - {oracle_tbl_name}")
        except Exception as e:
            logging.error(f"Error inserting {len(elements)} elements to table: {oracle_tbl_name} - {e}")

    def finish_bundle(self):
        # logging.info(f'Data loaded to Oracle for {tbl_name}')
        self.cursor.close()
        self.connection.close()


def run():
    pipeline_args = [
       "--project", config['env']['v_proc_project_id'],
       "--service_account_email", config['env']['v_df_atos_serviceaccountemail'],
       "--job_name", jobname.replace("_","-"),
       "--runner", config['env']['v_runner'],
       "--network", config['env']["v_network"],
       "--subnetwork", config['env']["v_subnetwork"],
       "--staging_location", config['env']["v_dfstagebucket"],
       "--temp_location", config['env']["v_gcs_temp_bucket"],
       "--region", config['env']["v_region"],
       "--save_main_session",
       "--num_workers", str(config['env']["v_numworkers"]),
       "--max_num_workers","10",
       "--no_use_public_ips",
       "--dataflow_service_options, enable_prime",
       "--autoscaling_algorithm","THROUGHPUT_BASED",
       "--worker_machine_type",  "n2-highmem-2",
       "--setup_file", '{}setup.py'.format(utilpath)
    ]

    if schema == 'P1':
        user = ra_external_data['v_user']
        passwd = access_secret(config['env']['v_pwd_secrets_url'] + ra_external_data['v_pwd_secret_name_p1'])
        host_name = ra_external_data['hostname']
        if config['env']['v_env_name'] == 'prod':
            service_name = ra_external_data['servicename_p1']
        else:
            service_name = ra_external_data['servicename_d5']
        conn_str='{}/{}@{}/{}'.format(user,passwd,host_name,service_name)
    elif schema == 'P2':
        user = ra_external_data['v_user']
        passwd = access_secret(config['env']['v_pwd_secrets_url'] + ra_external_data['v_pwd_secret_name_p2'])
        host_name = ra_external_data['hostname']
        if config['env']['v_env_name'] == 'prod':
            service_name = ra_external_data['servicename_p2']
        else:
            service_name = ra_external_data['servicename_d6']
        conn_str='{}/{}@{}/{}'.format(user,passwd,host_name,service_name)

    logging.info("Connection string used is :{}".format(conn_str))
    logging.info(f'Using service {service_name}')
    logging.info(f"Using secret with name {config['env']['v_pwd_secrets_url']}")

    try:
        logging.info(
           "===Apache Beam Run with pipeline options started===")
        pcoll = beam.Pipeline(argv=pipeline_args)
        if config['env']['v_runner'] == 'DataflowRunner':
            truncate = ( pcoll|"Initialize" >> beam.Create([None])
                    |"Truncate Oracle tables" >> beam.ParDo(TruncateOracleTables(conn_str))
            )
            rows = (
                pcoll 
                    | "Read from BQ" >>  beam.io.ReadFromBigQuery(query=bq_query, use_standard_sql=True, temp_dataset = temp_loc)
                    | 'TransformToTuple' >> beam.ParDo(TransformToTuple())
                )
            
            batch = (
                rows | 'Create Batches' >> beam.BatchElements(min_batch_size = 100000, max_batch_size=500000)
            )

            batch | 'Write to Oracle' >> beam.ParDo(WriteToOracle(conn_str))

            p = pcoll.run()
            dataflow_job_id = p.job_id()
            logging.info("===Submitting Asynchronous Dataflow Job===")

            
        else:
            truncate = ( pcoll|"Initialize" >> beam.Create([None])
                    |"Truncate Oracle tables" >> beam.ParDo(TruncateOracleTables(conn_str))
            )
            rows = (
                pcoll 
                    | "Read from BQ" >>  beam.io.ReadFromBigQuery(query=bq_query, use_standard_sql=True, temp_dataset = temp_loc)
                    | 'TransformToTuple' >> beam.ParDo(TransformToTuple())
                )
            
            batch = (
                rows | 'Create Batches' >> beam.BatchElements(min_batch_size = 100000, max_batch_size=500000)
            )

            batch | 'Write to Oracle' >> beam.ParDo(WriteToOracle(conn_str))

            p = pcoll.run()
            dataflow_job_id = p.job_id()
            logging.info("===Submitting Asynchronous Dataflow Job===")
        return dataflow_job_id

    except:
        logging.exception("===ERROR: Apache Beam Run Failed===")
        raise SystemExit()


def main(tbl_name):
    global tblschema
    global bq_query
    global jobname
    global temp_loc
    
    bq_tbl_name = f"{tbl_name}_{schema}".lower()
    bqproject_id = config['env']['v_curated_project_id']
    bqclient = bigquery.Client(bqproject_id)
    dataset_ref = bqclient.dataset(bq_stage_dataset)
    table_ref = dataset_ref.table(bq_tbl_name)
    table = bqclient.get_table(table_ref)
    f = io.StringIO("")
    bqclient.schema_to_json(table.schema, f)
    tblschema = json.loads(f.getvalue())
    # schema_copy = copy.deepcopy(tblschema)

    temp_loc = bigquery.DatasetReference(bqproject_id , bq_stage_dataset)
    temp_loc.projectId =bqproject_id
    temp_loc.datasetId=bq_stage_dataset


    bq_query = f'select * from {bqproject_id}.edwra_staging.{bq_tbl_name}'
    logging.info(bq_query)

    randomstring = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))

    jobname ="ra-concuity-load-to-oracle" + '-' + tbl_name.lower() + '-' + schema.lower() + '-' + time.strftime("%Y%m%d%H%M%S") #+ '-' + randomstring

    logging.info("===Job Name is {} ===".format(jobname))
    p1 = run()
    dataflow_job_id = p1

    logging.info("===END: Data Pipeline for load to Concuity table {} at {}===".format(
        tbl_name, time.strftime("%Y%m%d-%H:%M:%S")))

    print(dataflow_job_id)

    return dataflow_job_id

if __name__ =="__main__" :
    logging.getLogger().setLevel(logging.DEBUG)
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--src_sys_config_file", required=True,
                        help=("Source System Based Config file name"))
    parser.add_argument("--tbl_name", required=True,
                        help=("Concuity table name"))
    parser.add_argument("--schema", required=True,
                        help=("Concuity schema"))
    
    args = parser.parse_args()

    global srcsys_config_file
    srcsys_config_file = args.src_sys_config_file

    global ra_external_data
    ra_external_data = call_config_yaml(args.src_sys_config_file,"ra_external_data_config")

    global tbl_name
    tbl_name = args.tbl_name

    global schema
    schema = args.schema

    global bq_stage_dataset
    bq_stage_dataset = config['env']['v_parallon_ra_stage_dataset_name']

    global bq_suffix
    bq_suffix = config['env']['v_bq_suffix']

    logging.info("===BEGIN: Data Pipeline for load to Concuity tables in Oracle at {}===".format(
        time.strftime("%Y%m%d-%H:%M:%S")))
    
    main(tbl_name)