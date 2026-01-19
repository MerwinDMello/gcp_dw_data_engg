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
import decimal

timezone = pendulum.timezone("US/Central")

cwd = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.dirname(cwd)
sys.path.insert(0, base_dir)
utilpath = base_dir + '/utils/'
config_folder = base_dir + "/config/"

lob = "ra"
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
        jdkfile = reporting_config['v_jdkfile']
        gcsjarbucket = config['env']['v_dfjarbucket']
        jdkversion = reporting_config['v_jdkversion']
        jdbcjar = reporting_config['v_jdbc_jar']

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

    def readbqwriteclaimstable(self, sql_tgttablename):
        import jaydebeapi
        import pendulum
        import pandas_gbq
        import io
        from jaydebeapi import _DEFAULT_CONVERTERS
        from sqlalchemy import create_engine
        import datetime
        import time
        import oracledb

        jdbc_class_name = reporting_config['v_jdbc_class_name']
        jdbc_jar = reporting_config['v_jdbc_jar']
        tableinfo = reporting_config[src_db_sql]
        logging.info("===tableinfo is {} ===".format(tableinfo))
        
        bqsqlsqry = tableinfo.split("~")[0]

        if "where_condition" in bqsqlsqry:
            bqsqlsqry = bqsqlsqry.replace("where_condition", src_where_condition) 


        logging.info("===bqsqlsqry is {} ===".format(bqsqlsqry))
        #global schema
        sql_task = tableinfo.split("~")[1]
        #schema = tableinfo.split("~")[2]

        batch_size = int(tableinfo.split("~")[3])
        #logging.info("===batch_size: {}===".format(batch_size))
        bqsqlsqry = bqsqlsqry.replace('v_currtimestamp', str(pendulum.now(timezone))[:23])
        #bqsqlsqry = bqsqlsqry.replace('v_currtimestamp',pendulum.now(timezone).format('YYYY-MM-DD HH:mm:ss.SSS'))
        logging.info("===Executing SQL : {}===".format(bqsqlsqry))

        #creds = json.loads(access_secret(config['env']['v_secrets_url'] + reporting_config['v_secret_name']))

        #logging.info(creds)
        
        #username = reporting_config["v_user"]
        #passwd = access_secret(config['env']['v_pwd_secrets_url'] + reporting_config['v_pwd_secret_name_p1'])
        src_server_name = reporting_config["v_server_name"]
        src_db_name = reporting_config['v_database']
        src_db_port = reporting_config['v_port']
        src_db_type = reporting_config['v_databasetype']
        host_name = f"{src_server_name}:{src_db_port}"
        if schema == 'p1':
            username = reporting_config['v_user']
            passwd = access_secret(config['env']['v_pwd_secrets_url'] + reporting_config['v_pwd_secret_name_p1'])
            if config['env']['v_env_name'] == 'prod':
                service_name = reporting_config['servicename_p1']
            else:
                service_name = reporting_config['servicename_d5']

        if schema == 'p2':
            username = reporting_config['v_user']
            passwd = access_secret(config['env']['v_pwd_secrets_url'] + reporting_config['v_pwd_secret_name_p2'])
            if config['env']['v_env_name'] == 'prod':
                service_name = reporting_config['servicename_p2']
            else:
                service_name = reporting_config['servicename_d6']

        if src_db_type == 'sqlserver':
            jdbc_url = f"jdbc:sqlserver://{src_server_name}:{src_db_port};databaseName={src_db_name};encrypt=false;trustServerCertificate=true"
        elif src_db_type == 'cache':
            jdbc_url = f"jdbc:Cache://{src_server_name}:{src_db_port}/{src_db_name}"
        elif src_db_type == 'db2':
            jdbc_url = f"jdbc:db2://{src_server_name}:{src_db_port}/{src_db_name}"
        elif src_db_type == 'oracle':
            #jdbc_url = f"jdbc:oracle:thin:@//{src_server_name}:{src_db_port}/{src_db_name}"
            jdbc_url = '{}/{}'.format(host_name, service_name)
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

        conn = oracledb.connect(user=username, password=passwd, dsn=jdbc_url)

       # conn = jaydebeapi.connect(jdbc_class_name, jdbc_url, {
        #    'user': username, 'password': passwd}, jdbc_lib_path + jdbc_jar, jdbc_lib_path)
                
        #except_clause = 'except (individual_type_code, active_ind)'

        ## UPDATE COLUMN NAMES THAT DIFFER FROM BQ TO claims
        df = pd.read_gbq(bqsqlsqry,  project_id=bqproject_id)
       
        # df['processed_date'] = pd.to_datetime(df['processed_date'])
        logging.info(f"{df.dtypes}' pre df data types")
        # df['processed_date'] = df['processed_date'].dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:-3]
        logging.info(f"{df.head()}' df head")
        #df[df.select_dtypes(include=['datetime64']).columns] = df.select_dtypes(include=['datetime64']).apply(lambda x: x.dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:-3]) 
        # df['bill_dt'] = df['bill_dt'].dt.strftime('%Y-%m-%d')
        df["creation_dt"] = pd.to_datetime(df["creation_dt"])
        logging.info(f"{df.dtypes}' post df data types")

        cursor = conn.cursor()        
        if sql_task=="truncate":
            truncate_query = f"DELETE FROM {src_db_name}.{sql_tgttablename} WHERE rpt_name ='GRGLRECON'"
            logging.info(f" truncate_query:{truncate_query}")
      
            try:
                cursor.execute(truncate_query)
                conn.commit()
                logging.info(f"{sql_tgttablename}' truncated in oracle")
            except Exception as e:
                logging.error('Could not truncate table')
                exit
            
            columns = ', '.join(df.columns)
            placeholders = ', '.join([f":{i+1}" for i in range(len(df.columns))])
            insert_query = f"INSERT INTO {src_db_name}.{sql_tgttablename} ({columns}) VALUES ({placeholders})"
            logging.info(insert_query)
            
            for start in range(0, len(df), batch_size):
                batch = df[start: start+batch_size]
                batched_data = [tuple(None if pd.isna(x) else x for x in row) for row in batch.itertuples(index=False)]
                # logging.info(batched_data)
                try:
                    cursor.executemany(insert_query, batched_data)
                    conn.commit()
                    logging.info(f"{len(batched_data)} rows loaded to oracle")
                except jaydebeapi.Error as e:
                    logging.error(f"Failed to load batch: {e}. Trying individual rows")
                    for row in batched_data:
                        try:
                            cursor.execute(insert_query, row)
                            conn.commit()
                        except jaydebeapi.Error as e:
                            logging.error(f"Could not load {row} -- {e}")

        if sql_task=="insert":           
            ## WRITE TO ORACLE IN BATCHES
            #cursor = conn.cursor()
            columns = ', '.join(df.columns)
            placeholders = ', '.join([f":{i+1}" for i in range(len(df.columns))])
            insert_query = f"INSERT INTO {src_db_name}.{sql_tgttablename} ({columns}) VALUES ({placeholders})"
            #insert_query = f"INSERT INTO dbo.{sql_tgttablename} ({', '.join(df.columns)}) VALUES ({', '.join(['?'] * len(df.columns))})"
            logging.info(insert_query)
            
            for start in range(0, len(df), batch_size):
                batch = df[start: start+batch_size]
                batched_data = [tuple(None if pd.isna(x) else x for x in row) for row in batch.itertuples(index=False)]
                # logging.info(batched_data)
                try:
                    cursor.executemany(insert_query, batched_data)
                    conn.commit()
                    logging.info(f"{len(batched_data)} rows loaded to oracle")
                except jaydebeapi.Error as e:
                    logging.error(f"Failed to load batch: {e}. Trying individual rows")
                    for row in batched_data:
                        try:
                            cursor.execute(insert_query, row)
                            conn.commit()
                        except jaydebeapi.Error as e:
                            logging.error(f"Could not load {row} -- {e}")  

        cursor.close()
        conn.close()

        # df.to_sql(sql_tgttablename, con=engine, if_exists="append", index=False)
        
    def process(self, element):

        try:
            global bqproject_id
            bqproject_id = config['env']['v_curated_project_id']
           

            # read input table list and process each table in sequence

            self.readbqwriteclaimstable( sql_tgttablename)

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
        "--worker_machine_type", reporting_config['v_machine_type'],
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

def main(sql_tgttablename):
    global bq_query
    global jobname  

    if srctablename :
        jobname ="rarep-outbound-load-to-oracle" + '-' + srctablename.lower() + '-' + '-' + time.strftime("%Y%m%d%H%M%S") 
    else :
        jobname ="rarep-outbound-load-to-oracle" + '-' + schema + '-' + sql_tgttablename.lower() + '-' + '-' + time.strftime("%Y%m%d%H%M%S") 


    logging.info("===Job Name is {} ===".format(jobname))
    p1 = run()
    dataflow_job_id = p1

    logging.info("===END: Data Pipeline for load to claims table {} at {}===".format(
        sql_tgttablename, time.strftime("%Y%m%d-%H:%M:%S")))

    print(dataflow_job_id)

    return dataflow_job_id

if __name__ =="__main__" :
    logging.getLogger().setLevel(logging.DEBUG)
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument("--src_sys_config_file", required=True,
                        help=("src_sys_config_file"))
    parser.add_argument("--src_db_sql", required=True,
                        help=("Outbound Sql Query"))
    parser.add_argument("--tgttablename", required=True,
                        help=("SQL_claims target table name"))
    parser.add_argument("--src_system", required=True,
                        help=("Source System"))
    parser.add_argument("--beam_runner", required=True,
                        help=("Beam Runner"))
    parser.add_argument("--src_where_condition", required=False,
                        help=("Batch Process Condition"))
    parser.add_argument("--src_jdbc_lib_path", required=False,
                        help=("JDBC Lib Path"))
    parser.add_argument("--srctablename", required=False,
                        help=("Source Table Name"))
    parser.add_argument("--schema", required=True,
                        help=("schema Name"))
    
    args = parser.parse_args()

    global sourcesysnm
    sourcesysnm = args.src_system

    global srctablename
    srctablename = args.srctablename

    global reporting_config    
    reporting_config = call_config_yaml(config_folder,args.src_sys_config_file)

    global sql_tgttablename
    sql_tgttablename = args.tgttablename

    global schema
    schema = args.schema
 

    global src_db_sql
    src_db_sql = args.src_db_sql
    logging.info("===src_db_sql is {} ===".format(src_db_sql))

    global src_where_condition
    src_where_condition = args.src_where_condition

    logging.info("===src_where_condition is {} ===".format(src_where_condition))

    global src_jdbc_lib_path
    src_jdbc_lib_path = args.src_jdbc_lib_path

    global beam_runner
    beam_runner = args.beam_runner

    logging.info(beam_runner)

    logging.info("===BEGIN: Data Pipeline for load to claims table {} in SQL SERVER at {}===".format(
        sql_tgttablename,time.strftime("%Y%m%d-%H:%M:%S")))
    
    main(sql_tgttablename)