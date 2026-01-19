import json 
import yaml
from datetime import datetime, timedelta, timezone
import os,sys
import pandas as pd
import pendulum
import logging
import traceback
import re
import random
import time
import argparse
import string
import apache_beam as beam
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import secretmanager

timezone = pendulum.timezone("US/Central")

cwd = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.dirname(cwd)
sys.path.insert(0, base_dir)
utilpath = base_dir + '/utils/'
config_folder = base_dir + "/config/"

lob = "edwrmt"
#lob = lob.lower().strip()
#lob_abbr = lob.replace("edw","")

def call_config_yaml(config_folder, filename):
    # load input config .yaml file from config folder
    cfgfile = open(config_folder + filename)
    default_config = yaml.safe_load(cfgfile)
    default_config = json.loads(json.dumps(default_config, default=str))
    config = default_config
    return config

config = call_config_yaml(config_folder, f"{lob}_config.yaml")

current_timezone = pendulum.timezone("US/Central")

def access_secret(secret_resourceid):
    logging.info("Get Secret Version {}".format(secret_resourceid))
    client = secretmanager.SecretManagerServiceClient()
    payload = client.access_secret_version(name=secret_resourceid).payload.data.decode("UTF-8")
    return payload

class setuprunnerenv(beam.DoFn):
    def process(self, context):
        jdkfile = remits_config['v_jdkfile']
        gcsjarbucket = config['env']['v_dfjarbucket']
        jdkversion = remits_config['v_jdkversion']
        jdbcjar = remits_config['v_jdbc_jar']

        # use /tmp/ on dataflow worker node for processing
        global base_dir
        base_dir = '/tmp/'
        os.system('gsutil cp ' + gcsjarbucket +
                  jdbcjar + ' ' + base_dir + ' && ls ')

        # Copy required java libraries
        os.system('gsutil cp ' + gcsjarbucket +
                  jdkfile + ' ' + base_dir + ' && ls ')

        # setup jvm path and java version
        os.system('mkdir -p /usr/lib/jvm')
        os.system('tar xvzf ' + base_dir + jdkfile + ' -C /usr/lib/jvm')
        os.system(
            'update-alternatives --install "/usr/bin/java" "java" "/usr/lib/jvm/' + jdkversion + '/bin/java" 1 ')
        os.system('update-alternatives --config java')
        logging.info('JDK Libraries copied to Instance..')

        os.system('java -version')

        return list("1")

class bqtojdbc(beam.DoFn):

    def readbqwriteremitstable(self, sql_truncatetablelist):
        import jaydebeapi
        import pendulum
        import pandas_gbq
        import io
        from jaydebeapi import _DEFAULT_CONVERTERS
        from sqlalchemy import create_engine
        import datetime
        import time

        jdbc_class_name = remits_config['v_jdbc_class_name']
        jdbc_jar = remits_config['v_jdbc_jar']
  
        creds = json.loads(access_secret(config['env']['v_secrets_url'] + remits_config['v_secret_name']))

        logging.info(creds)
        username = creds["user"]
        passwd = creds["password"]
        src_server_name = creds["server"]
        src_db_name = remits_config['v_database']
        src_db_port = remits_config['v_port']
        src_db_type = remits_config['v_databasetype']

        if src_db_type == 'sqlserver':
            jdbc_url = f"jdbc:sqlserver://{src_server_name}:{src_db_port};databaseName={src_db_name};encrypt=false;trustServerCertificate=true"
        elif src_db_type == 'cache':
            jdbc_url = f"jdbc:Cache://{src_server_name}:{src_db_port}/{src_db_name}"
        elif src_db_type == 'db2':
            jdbc_url = f"jdbc:db2://{src_server_name}:{src_db_port}/{src_db_name}"
        elif src_db_type == 'oracle':
            jdbc_url = f"jdbc:oracle:thin:@//{src_server_name}:{src_db_port}/{src_db_name}"
        elif src_db_type == 'teradata':
            jdbc_url = f"jdbc:teradata://{src_server_name}/database={src_db_name},dbs_port={src_db_port}"
        else:
            jdbc_url = f"jdbc:sqlserver://{src_server_name}:{src_db_port};databaseName={src_db_name};encrypt=false;trustServerCertificate=true"

        # set jdbc lib path to /tmp/ on Dataflow runner
        if beam_runner == 'DataflowRunner':
            jdbc_lib_path = base_dir
        else:
            jdbc_lib_path = src_jdbc_lib_path


        logging.info("=== JDBC Lib Path {} ===".format(str(jdbc_lib_path)))
        logging.info("=== JDBC URL {} ===".format(str(jdbc_url)))

        conn = jaydebeapi.connect(jdbc_class_name, jdbc_url, {
            'user': username, 'password': passwd}, jdbc_lib_path + jdbc_jar, jdbc_lib_path)
                
        cursor = conn.cursor()
        logging.info("===sql_truncatetablelist is {} ===".format(sql_truncatetablelist))
        for tbl in sql_truncatetablelist:
            truncate_query = f"TRUNCATE TABLE dbo.{tbl}"
            logging.info(f" truncate_query:{truncate_query}")
            try:
                cursor.execute(truncate_query)
                conn.commit()
            except Exception as e:
                logging.error("=== Could not truncate table {}===".format(tbl))
                exit
        logging.info(
                    "===Truncated table {}===".format(tbl))
        ## UPDATE COLUMN NAMES THAT DIFFER FROM BQ TO remits
       
        

        cursor.close()
        conn.close()

        # df.to_sql(sql_truncatetablelist, con=engine, if_exists="append", index=False)
        
    def process(self, element):

        try:
            global bqproject_id
            bqproject_id = config['env']['v_curated_project_id']
           

            # read input table list and process each table in sequence

            self.readbqwriteremitstable( sql_truncatetablelist)

            logging.info("===END of processing tablelist at {}".format(
                time.strftime("%Y%m%d-%H:%M:%S")))

        except:
            logging.error(
                "===ERROR: Failure occurred within Process function===")
            logging.error(traceback.format_exc())
            raise SystemExit()
    
def run():
    pipeline_args = [
        "--project", config['env']['v_proc_project_id'],
        "--service_account_email",  config['env']['v_serviceaccountemail'],
        "--job_name", jobname.replace("_","-"),
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
        "--worker_machine_type", remits_config['v_machine_type'],
        "--setup_file", '{}setup.py'.format(utilpath)

    ]

    try:
        logging.info(
            "===Apache Beam Run with pipeline options started===")
        pcoll = beam.Pipeline(argv=pipeline_args)
        
        if beam_runner == 'DataflowRunner':
            pcoll | "Initialize" >> beam.Create(["1"]) | 'Setup Dataflow Worker' >> beam.ParDo(
                setuprunnerenv()) | "Read JDBC, Write to Google BigQuery" >> beam.ParDo(bqtojdbc())
        else:
            pcoll | "Initialize.." >> beam.Create(
                ["1"]) | "Read JDBC, Write to Google BigQuery" >> beam.ParDo(bqtojdbc())

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

def main(sql_truncatetablelist):
    global bq_query
    global jobname    

    jobname ="rmt-outbound-truncate-tables" + '-' + time.strftime("%Y%m%d%H%M%S") 

    logging.info("===Job Name is {} ===".format(jobname))
    p1 = run()
    dataflow_job_id = p1

    logging.info("===END: Data Pipeline for load to remits table {} at {}===".format(
        sql_truncatetablelist, time.strftime("%Y%m%d-%H:%M:%S")))

    print(dataflow_job_id)

    return dataflow_job_id

if __name__ =="__main__" :
    logging.getLogger().setLevel(logging.DEBUG)
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument("--src_sys_config_file", required=True,
                        help=("src_sys_config_file"))    
    parser.add_argument("--truncatetablelist_str", required=True,
                        help=("SQL_remits target table name"))
    parser.add_argument("--src_system", required=True,
                        help=("Source System"))
    parser.add_argument("--beam_runner", required=True,
                        help=("Beam Runner"))
    parser.add_argument("--src_jdbc_lib_path", required=False,
                        help=("JDBC Lib Path"))
    
    
    args = parser.parse_args()

    global sourcesysnm
    sourcesysnm = args.src_system

    global remits_config
    remits_config = call_config_yaml(config_folder,args.src_sys_config_file)    

    global sql_truncatetablelist
    sql_truncatetablelist = args.truncatetablelist_str.split(',')
    logging.info("===sql_truncatetablelist is {} ===".format(sql_truncatetablelist))

    global beam_runner
    beam_runner = args.beam_runner

    logging.info(beam_runner)
    global src_jdbc_lib_path
    src_jdbc_lib_path = args.src_jdbc_lib_path

    logging.info(src_jdbc_lib_path)

    logging.info("===BEGIN: Data Pipeline for truncating remits table {} in SQL SERVER at {}===".format(
        sql_truncatetablelist,time.strftime("%Y%m%d-%H:%M:%S")))
    
    main(sql_truncatetablelist)