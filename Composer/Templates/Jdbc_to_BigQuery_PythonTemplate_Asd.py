import logging
import os
import time
import traceback
from datetime import datetime as dt
from datetime import timedelta
from google.cloud import bigquery
import apache_beam as beam
import sys
import pandas as pd
import yaml
import json
from google.cloud import secretmanager
import argparse
import string
import random
import ast
import re
import numpy as np
import pendulum
import datetime as dtme
import pyarrow as pa
import pandas_gbq



cwd = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.dirname(cwd)
sys.path.insert(0, base_dir)
utilpath = base_dir + '/utils/'
config_folder = base_dir + "/config/"
# job_run_count =  1
#global validationsql_folder
#validationsql_folder = base_dir + "/sql/validation_sql/"


def call_config_yaml(filename, variablename):
    # load input config .yaml file from config folder
    cfgfile = open(config_folder + filename)
    default_config = yaml.safe_load(cfgfile)
    default_config = json.loads(json.dumps(default_config, default=str))
    config = default_config
    return config


config = call_config_yaml("asd_config.yaml", "hca_asd_default_vars")


def access_secret(secret_resourceid):
    #logging.info("Get Secret Version {}".format(secret_resourceid))
    client = secretmanager.SecretManagerServiceClient()
    payload = client.access_secret_version(
        secret_resourceid).payload.data.decode("UTF-8")
    return payload

# List of datetime formats that you want to handle
datetime_formats = [
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    # Add any other datetime formats that your data might have
]


def to_str(x):
    if pd.isnull(x):
        return None
    else:
        return str(x)

def to_int(x):
    try:
        return int(x)
    except:
        return np.nan

def convert_datetime(x):
    if pd.isnull(x):
        return np.nan
    
    if x == None:
        return np.nan
    
    for fmt in datetime_formats:
        try:
            # If the datetime string can be converted using the current format, do the conversion
            dte = dtme.datetime.strptime(str(x), fmt)
            # Then convert it to the desired format and return
            return dtme.datetime(dte.year, dte.month, dte.day, dte.hour, dte.minute, dte.second)
        except:
            continue
    
    # Print an error message if none of the formats match (you could also raise an error or handle this differently depending on your needs)
    print(f"Error: Could not convert '{x}' to datetime")
    return np.nan
    
class setuprunnerenv(beam.DoFn):
    def process(self, context):
        jdkfile = srcsys_config['v_jdkfile']
        gcsjarbucket = config['env']['v_dfjarbucket']
        jdkversion = srcsys_config['v_jdkversion']
        jdbcjar = srcsys_config['v_jdbc_jar']

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


class jdbctobq(beam.DoFn):

    def readjdbcwritebqtable(self, tableinfo, bqproject_id):
        import jaydebeapi
        import pendulum
        import pandas_gbq
        import io
        from jaydebeapi import _DEFAULT_CONVERTERS
        import datetime
        import decimal
        decimal_context = decimal.Context(prec=13)
        def _to_datetime(rs, col):

            java_val = rs.getTimestamp(col)
            if not java_val:
                return
            d = datetime.datetime.strptime(str(java_val)[:19], "%Y-%m-%d %H:%M:%S")
            d = d.replace(microsecond=java_val.getNanos() // 1000)
            return str(d)

        _DEFAULT_CONVERTERS.update({"TIMESTAMP": _to_datetime })

        try:

            timezone = pendulum.timezone("US/Central")
            #timezone = pendulum.timezone("Asia/Calcutta")

            jdbc_class_name = srcsys_config['v_jdbc_class_name']
            jdbc_url = f'jdbc:sqlserver://{src_server_name}.HCA.CORPAD.NET;encrypt=false'               
            jdbc_jar = srcsys_config['v_jdbc_jar']

            username = srcsys_config['v_user']
            passwd = access_secret(
                config['env']['v_pwd_secrets_url'] + srcsys_config['v_pwd_secret_name'])

            # set jdbc lib path to /tmp/ on Dataflow runner
            if config['env']['v_runner'] == 'DataflowRunner':
                jdbc_lib_path = base_dir
            else:
                jdbc_lib_path = srcsys_config['v_jdbc_lib_path']

            conn = jaydebeapi.connect(jdbc_class_name, jdbc_url, {
                'user': username, 'password': passwd}, jdbc_lib_path + jdbc_jar, jdbc_lib_path)

            # read input table info and extract table/query details
            srctableid = tableinfo.split("~")[0]
            srctablename = tableinfo.split("~")[1]
            tgttablename = tableinfo.split("~")[2]

            # replace schema names in bq target table list with values from env config file
            tgttablename = tgttablename.replace(
                'v_asd_stage_dataset_name', config['env']['v_asd_stage_dataset_name'])
            tgttablename = tgttablename.replace(
                'v_asd_core_dataset_name', config['env']['v_asd_core_dataset_name'])

            tgttableloadtype = tableinfo.split("~")[3]
            srctablequery = tableinfo.split("~")[4]

            srctablequery = srctablequery.replace(
                'v_currtimestamp', str(pendulum.now(timezone).strftime("%Y-%m-%d %H:%M:%S")))

            if src_server_name:
                srctablequery = srctablequery.replace('v_server_name', src_server_name)
                srctablename = srctablename.replace('v_server_name', src_server_name)
        
            if src_from_date:
                srctablequery = srctablequery.replace('v_from_date', src_from_date)
                srctablename = srctablename.replace('v_from_date', src_from_date)


            v_chunksize = int(tableinfo.split("~")[5])
            srctablecountquerywhereclause = srctablequery.split("time from ")[1]
            srctablecountquery = "select count(1) as SRC_COUNT from " + srctablecountquerywhereclause
            # src_rec_count = pd.read_sql(srctablecountquery, conn)
            # src_rec_count = src_rec_count['SRC_COUNT'][0]
            # src_rec_count = 0

            logging.info("===Starting process to  extract {} and load {} at {}===".format(
                srctablename, tgttablename, time.strftime("%Y%m%d-%H:%M:%S")))
            # some characters in column names need to be replaced
            col_spl_char = ['.', '[', ']']

            load_count = 0

            # if job_run_count == 1:
            #     job_run_count = 2
            # else:
            #     # raising system exit so no duplicate records are created.
            #     logging.info("the table list is re-ran, please refresh all staging tables for this table list for server: "+src_server_name)
            #     raise SystemExit()

            # Read source query using jdbc connection , rename columns if needed and write to bq table
            tableload_start_time = str(pendulum.now(timezone))[:23]

            try:

                bq_dataset = tgttablename.split('.')[0]
                tgt_bq_table = tgttablename.split('.')[1]
                bq_project_id = config['env']['v_curated_project_id']
                bqclient = bigquery.Client(bq_project_id)
                job_config = bigquery.job.LoadJobConfig()
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
                dataset_ref = bqclient.dataset(bq_dataset)
                table_ref = dataset_ref.table(tgt_bq_table)
                table = bqclient.get_table(table_ref)
                f = io.StringIO("")
                bqclient.schema_to_json(table.schema, f)
                tblschema = json.loads(f.getvalue())
                logging.info(tblschema)

                tgt_bq_table = bq_dataset + '.' + tgt_bq_table
                
                for db in src_db_dict:
                    load_count = 0
                    tableload_start_time = str(pendulum.now(timezone))[:23]


                    srctablequery_input = srctablequery.replace('v_db_name', db['name'][1])
                    srctablequery_input = srctablequery_input.replace('v_encrypt', db['name'][2])
                    srctablequery_input = srctablequery_input.replace('v_coid', db['name'][0])

                    srctablename_input = srctablequery_input.replace('v_db_name', db['name'][1])
                    srctablename_input = srctablequery_input.replace('v_encrypt', db['name'][2])
                    srctablename_input = srctablequery_input.replace('v_coid', db['name'][0])
                    logging.info(srctablename_input)
                    logging.info("===Starting process to  extract {} and load {} at {} from db {}===".format(
                    srctablename_input, tgttablename, time.strftime("%Y%m%d-%H:%M:%S"),db['name'][1]))

                    srctablecntquery_input = srctablecountquery.replace('v_db_name', db['name'][1])
                    srctablecntquery_input = srctablecntquery_input.replace('v_encrypt', db['name'][2])
                    srctablecntquery_input = srctablecntquery_input.replace('v_coid', db['name'][0])

                    srctablecntquery_input = srctablecntquery_input.replace('v_db_name', db['name'][1])
                    srctablecntquery_input = srctablecntquery_input.replace('v_encrypt', db['name'][2])
                    srctablecntquery_input = srctablecntquery_input.replace('v_coid', db['name'][0])

                    src_rec_count = pd.read_sql(srctablecntquery_input, conn)
                    src_rec_count = src_rec_count['SRC_COUNT'][0]
                    

                    for chunk in pd.read_sql(srctablequery_input, conn, chunksize=v_chunksize):
                        chunk.columns = map(str.lower, chunk.columns)
                        for char in col_spl_char:
                            chunk.columns = chunk.columns.str.replace(
                                char, '_', regex=False)
                                
                        logging.info(chunk.info())

                        for i in range(0, len(tblschema)):
                            dtype = tblschema[i]['type'].lower()
                                
                            if dtype == 'time':
                                dtype = 'datetime64[ns]'
                            elif dtype == 'integer':
                                dtype = 'Int64'
                            elif dtype == 'numeric':
                                dtype = 'float64'
                            elif dtype == 'string':
                                dtype = 'str'   
                                                      
                            if dtype == 'float64':
                                chunk[tblschema[i]['name']] = chunk[tblschema[i]['name']].apply(dtype)
                                chunk[tblschema[i]['name']] = chunk[tblschema[i]['name']].apply(decimal_context.create_decimal_from_float)

                            elif dtype == 'date':
                                chunk[tblschema[i]['name']] = chunk[tblschema[i]['name']].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d") if x is not None else np.nan)
                                    
                            elif dtype == 'datetime':                                                                           
                                chunk[tblschema[i]['name']] = chunk[tblschema[i]['name']].apply(convert_datetime)
                                if chunk[tblschema[i]['name']].dtype != 'object':
                                    chunk[tblschema[i]['name']] = pd.to_datetime(chunk[tblschema[i]['name']])
                                

                            elif dtype == 'Int64':

                                chunk[tblschema[i]['name']] = chunk[tblschema[i]['name']].apply(to_int)
                                chunk[tblschema[i]['name']] = chunk[tblschema[i]['name']].astype(dtype)

                            elif dtype == 'str':
                                chunk[tblschema[i]['name']] = chunk[tblschema[i]['name']].apply(to_str)


                            elif i <= len(chunk.columns):
                                chunk[tblschema[i]['name']] = chunk[tblschema[i]['name']].astype(dtype)                                      

                            elif i > len(chunk.columns):
                                chunk[tblschema[i]['name']] = np.nan
                                chunk[tblschema[i]['name']] = chunk[tblschema[i]
                                                                            ['name']].astype(dtype) 
                                  
                                                                   
                        for attempt in range(5):
                            try:
                                pandas_gbq.to_gbq(chunk, tgt_bq_table,
                                            project_id=bq_project_id, if_exists='append', table_schema=tblschema)
                                break
                            except pandas_gbq.exceptions.GenericGBQException as e:
                                logging.info("data load attempt {} failed".format(attempt))
                                if 'Exceeded rate limits' in str(e):
                                    wait_time = (5 ** attempt)
                                    time.sleep(wait_time)
                                else:
                                    logging.info("{} rows not loaded into table {} from server:{} db {}:".format(load_count,
                                           tgttablename,src_server_name,db['name'][1] ))
                                                               
                        load_count += len(chunk.index)

                    tableload_end_time = str(pendulum.now(timezone))[:23]
                    tableload_run_time = (pd.to_datetime(tableload_end_time) - pd.to_datetime(tableload_start_time))
                    tgt_rec_count = load_count
                    if src_rec_count == tgt_rec_count:
                        audit_status = 'PASS'
                    elif tgt_rec_count > src_rec_count:
                        audit_status = 'PASS(More records in Target)'
                    else:
                        audit_status = 'FAIL'
                    
                    tgt_count = pd.read_gbq(
                        "select count(1) as tgt_count from " + tgttablename + " where coid = '"+db['name'][0]+"'", project_id=bqproject_id)
                    tgt_count = tgt_count['tgt_count'][0]

                    srctablename_final = src_server_name +"."+db['name'][1]+"."+srctablename

                    audit_insert_stt = "insert into {} values (GENERATE_UUID() , {}, '{}', '{}', '{}', '{}',  {}, {}, '{}', '{}', '{}', '{}', '{}', '{}'  ) " .format(
                        config['env']['v_audittablename'],
                        srctableid,
                        sourcesysnm,
                        srctablename_final,
                        tgttablename,
                        'RECORD_COUNT',
                        src_rec_count,
                        tgt_rec_count,
                        tableload_start_time,
                        tableload_end_time,
                        tableload_run_time,
                        jobname,
                        str(pendulum.now(timezone))[:23],
                        audit_status

                    )

                    audit_entry = pd.read_gbq(
                        audit_insert_stt,  project_id=bqproject_id, max_results=0)
                    logging.info(
                        "===Audit entry added for srctableid {} - srctablename {} ===".format(srctableid, srctablename))

                    bqtable = tgttablename.split(".")[1]
                    # self.executevalidationsqls(bqtable)


                

            except Exception as e:
                logging.info("===Unable to pull any more rows : {} rows loaded into table {}===".format(
                    load_count, tgttablename))
                logging.info(traceback.format_exc())
                if 'the connection is closed' in str(e).lower() : 
                    raise SystemExit()
                elif srcsys_config['v_databasetype'] == 'sqlserver' : 
                    logging.info("Not Raising System Exit to handle com.ibm.db2.jcc.am.com.ibm.db2.jcc.am.SqlException \
                                 Invalid operation: result set  is closed. ERRORCODE=-4470, SQLSTATE=null")
                else :
                    raise SystemExit()
                    


        except:
            logging.error(traceback.format_exc())
            logging.error("===ERROR: Failure occurred within function===")
            raise SystemExit()

    def process(self, element):

        try:
            global bqproject_id
            bqproject_id = config['env']['v_curated_project_id']
            tablelist = srcsys_config[src_tbl_list]

            # read input table list and process each table in sequence

            num_tables = len(tablelist)
            logging.info("===Number of tables {}===".format(str(num_tables)))

            for tableinfo in tablelist:
                self.readjdbcwritebqtable(tableinfo, bqproject_id)

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
        "--job_name", jobname,
        "--runner", config['env']['v_runner'],
        "--network", config['env']["v_network"],
        "--subnetwork", config['env']["v_subnetwork"],
        "--staging_location", config['env']["v_dfstagebucket"],
        "--temp_location", config['env']["v_gcs_temp_bucket"],
        "--region", config['env']["v_region"],
        "--save_main_session",
        "--num_workers", str(config['env']["v_numworkers"]),
        "--max_num_workers",   str(config['env']["v_maxworkers"]),
        "--no_use_public_ips",
        "--dataflow_service_options, enable_prime",
        "--autoscaling_algorithm", "THROUGHPUT_BASED",
        "--worker_machine_type", srcsys_config['v_machine_type'],
        "--setup_file", '{}setup.py'.format(utilpath)

    ]

    try:
        logging.info(
            "===Apache Beam Run with pipeline options started===")
        pcoll = beam.Pipeline(argv=pipeline_args)
        if config['env']['v_runner'] == 'DataflowRunner':
            pcoll | "Initialize" >> beam.Create(["1"]) | 'Setup Dataflow Woker' >> beam.ParDo(
                setuprunnerenv()) | "Read JDBC, Write to Google BigQuery" >> beam.ParDo(jdbctobq())
        else:
            pcoll | "Initialize.." >> beam.Create(
                ["1"]) | "Read JDBC, Write to Google BigQuery" >> beam.ParDo(jdbctobq())

        p = pcoll.run()
        logging.info("===Apache Beam Run completed successfully===")
        dataflow_job_id = p.job_id()
        logging.info( "===Submitted Asynchronous Dataflow Job, Job id is " + dataflow_job_id + " ====")
        return dataflow_job_id

    except:
        logging.exception("===ERROR: Apache Beam Run Failed===")
        raise SystemExit()


def main(sourcesysnm, src_tbl_list):

    global jobname
    # job_run_count = 1

    randomstring = ''.join(random.choices(
        string.ascii_lowercase + string.digits, k=8))
    if src_server_name:
        jobname = sourcesysnm[:3] + "-p-" + src_server_name.lower() + '-' + src_tbl_list + '-' + time.strftime("%Y%m%d%H%M%S") + '-' + randomstring        
    else: 
        jobname = sourcesysnm[:3] + "-p-" + srcsys_config_file.split("/")[1][:-12].replace(
        '_', '-') + '-' + src_tbl_list + '-' + time.strftime("%Y%m%d%H%M%S") + '-' + randomstring
        
    logging.info("===Job Name is {} ===".format(jobname))
    p1 = run()
    dataflow_job_id = p1

    logging.info("===END: Data Pipeline for src_tbl_list {}-{} at {}===".format(
        sourcesysnm, src_tbl_list, time.strftime("%Y%m%d-%H:%M:%S")))
    print(dataflow_job_id)

    return dataflow_job_id
    


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--src_sys_config_file", required=True,
                        help=("Source System Based Config file name"))
    parser.add_argument("--src_sys_airflow_varname", required=True,
                        help=("Source System Based Config file name"))
    parser.add_argument("--src_tbl_list", required=True,
                        help=("Source Table List"))
    parser.add_argument("--src_db_list", required=False,
                        help=("Source Database List"))
    parser.add_argument("--src_server_name", required=False,
                        help=("Source Server Name"))
    parser.add_argument("--src_from_date", required=False,
                        help=("Source from date"))
   
    
    args = parser.parse_args()

    global srcsys_config_file
    srcsys_config_file = args.src_sys_config_file

    global srcsys_config
    srcsys_config = call_config_yaml(
        args.src_sys_config_file, args.src_sys_airflow_varname)

    global src_tbl_list
    src_tbl_list = args.src_tbl_list

    global src_db_list
    src_db_list = args.src_db_list

    global src_server_name
    src_server_name = args.src_server_name

    global src_from_date
    src_from_date = args.src_from_date


    global sourcesysnm
    sourcesysnm = srcsys_config['v_sourcesysnm']

    global src_db_dict

    src_table_list_str = src_db_list.replace("[{","[{\"")
    src_table_list_str = src_table_list_str.replace(":","\":")
    src_table_list_str = src_table_list_str.replace(": [",": [\"")
    src_table_list_str = src_table_list_str.replace(", ","\", \"")
    src_table_list_str = src_table_list_str.replace("]}","\"]}")
    src_table_list_str = src_table_list_str.replace("\"{","{\"")
    src_table_list_str = src_table_list_str.replace("}\",","},")
    logging.info(src_table_list_str)

    src_db_dict = json.loads(src_table_list_str)
    logging.info(src_db_dict)

    logging.info(src_db_list)
    # global job_run_count
    # job_run_count = 1

    for db in src_db_dict:
        logging.info(db['name'][1])

    logging.info("===BEGIN: Data Pipeline for src_tbl_list {}-{} at {}===".format(
        sourcesysnm, src_tbl_list, time.strftime("%Y%m%d-%H:%M:%S")))
    main(sourcesysnm, src_tbl_list)
    