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
import math
import random
# import ast
import re
import numpy as np
import decimal

cwd = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.dirname(cwd)
sys.path.insert(0, base_dir)
utilpath = base_dir + '/utils/'
config_folder = base_dir + "/config/"
#global validationsql_folder
#validationsql_folder = base_dir + "/sql/validation_sql/"


def call_config_yaml(filename):
    # load input config .yaml file from config folder
    cfgfile = open(config_folder + filename)
    default_config = yaml.safe_load(cfgfile)
    default_config = json.loads(json.dumps(default_config, default=str))
    config = default_config
    return config


config = call_config_yaml("edwclm_config.yaml")


def access_secret(secret_resourceid):
    #logging.info("Get Secret Version {}".format(secret_resourceid))
    client = secretmanager.SecretManagerServiceClient()
    print(secret_resourceid)
    # if beam_runner == 'DataflowRunner':
    #     payload = client.access_secret_version(
    #     secret_resourceid).payload.data.decode("UTF-8")
    # else:
    payload = client.access_secret_version(
    name=secret_resourceid).payload.data.decode("UTF-8")
    return payload

# List of datetime formats that you want to handle
datetime_formats = [
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S"
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

def to_decimal(value):
    try:
        return decimal.Decimal(value)
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
            dte = dt.strptime(x, fmt)
            # Then convert it to the desired format and return
            return dt(dte.year, dte.month, dte.day, dte.hour, dte.minute, dte.second)
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

    def executevalidationsqls(self, bq_table):
        bq_table = bq_table.lower()

        if beam_runner == 'DataflowRunner':
            sqlbucket = 'gs://' + \
                config['env']['v_dag_bucket_name'] + \
                '/dags/sql/validation_sql/'
            os.system('gsutil cp ' + sqlbucket + '*' + bq_table +
                      '.sql' + ' ' + base_dir + ' && ls ')
            logging.info(
                "===Copying Validation SQL's for table {} if any to dataflow runner===".format(bq_table))
            os.system('gsutil cp ' + sqlbucket + '*' + bq_table +
                      '.sql' + ' ' + base_dir + ' && ls ')
        else:
            logging.info(
                "===Checking for Validation SQL's for table {} if any locally===".format(bq_table))

        file_list = [a for a in os.listdir(
            base_dir) if a.endswith(bq_table + ".sql")]

        if file_list:
            for filename in file_list:
                bqsqlsqryfile = open(base_dir + '\\' + filename)
                bqsqlsqry = bqsqlsqryfile.read()
                logging.info("===Executing SQL : {}===".format(filename))
                df = pd.read_gbq(bqsqlsqry,  project_id=bqproject_id)
                logging.info(df)
        else:
            logging.info(
                "===Did not find any Validation SQL for table - {}===".format(bq_table))

    def readjdbcwritebqtable(self, tableinfo, bqproject_id):
        import jaydebeapi
        import pendulum
        import pandas_gbq
        import io
        from jaydebeapi import _DEFAULT_CONVERTERS
        import datetime
        import time
        decimal_context = decimal.Context(prec=13)
        # logging.info(base_dir)

        # Override jaydebeapi package default converter to fix milliseconds processing bug
        def _to_datetime(rs, col):

            # logging.info(
            #         "===Column before conversion {}===".format(str(col)))
            java_val = rs.getTimestamp(col)
            if not java_val:
                return
            d = datetime.datetime.strptime(str(java_val)[:19], "%Y-%m-%d %H:%M:%S")
            d = d.replace(microsecond=java_val.getNanos() // 1000)

            # logging.info(
            #         "===Column after conversion {}===".format(str(d)))

            return str(d)

        _DEFAULT_CONVERTERS.update({"TIMESTAMP": _to_datetime })

        try:

            dt1 = dt.now()
            timezone = pendulum.timezone("US/Central")
            #timezone = pendulum.timezone("Asia/Calcutta")

            jdbc_class_name = srcsys_config['v_jdbc_class_name']
            jdbc_jar = srcsys_config['v_jdbc_jar']

            creds = json.loads(access_secret(config['env']['v_secrets_url'] + srcsys_config['v_secret_name']))

            logging.info(creds)
            username = creds["user"]
            passwd = creds["password"]
            src_server_name = creds["server"]
            src_db_name = srcsys_config['v_database']
            src_db_port = srcsys_config['v_port']
            src_db_type = srcsys_config['v_databasetype']

            if src_db_type == 'sqlserver':
                jdbc_url = f"jdbc:sqlserver://{src_server_name}:{src_db_port};encrypt=false;trustServerCertificate=true"
            elif src_db_type == 'cache':
                jdbc_url = f"jdbc:Cache://{src_server_name}:{src_db_port}/{src_db_name}"
            elif src_db_type == 'db2':
                jdbc_url = f"jdbc:db2://{src_server_name}:{src_db_port}/{src_db_name}"
            elif src_db_type == 'oracle':
                jdbc_url = f"jdbc:oracle:thin:@//{src_server_name}:{src_db_port}/{src_db_name}"
            elif src_db_type == 'teradata':
                jdbc_url = f"jdbc:teradata://{src_server_name}/database={src_db_name},dbs_port={src_db_port}"
            else:
                jdbc_url = f"jdbc:sqlserver://{src_server_name}:{src_db_port};encrypt=false;trustServerCertificate=true"

            # set jdbc lib path to /tmp/ on Dataflow runner
            if beam_runner == 'DataflowRunner':
                jdbc_lib_path = base_dir
            else:
                jdbc_lib_path = src_jdbc_lib_path

            logging.info("=== JDBC Lib Path {} ===".format(str(jdbc_lib_path)))
            logging.info("=== JDBC URL {} ===".format(str(jdbc_url)))

            conn = jaydebeapi.connect(jdbc_class_name, jdbc_url, {
                'user': username, 'password': passwd}, jdbc_lib_path + jdbc_jar, jdbc_lib_path)

            # read input table info and extract table/query details
            srctableid = tableinfo.split("~")[0]
            srctablename = tableinfo.split("~")[1]
            tgttablename = tableinfo.split("~")[2]

            # replace schema names in bq target table list with values from env config file
            tgttablename = tgttablename.replace(
                'v_clm_stage_dataset_name', config['env']['v_clm_stage_dataset_name'])
            tgttablename = tgttablename.replace(
                'v_clm_core_dataset_name', config['env']['v_clm_core_dataset_name'])

            tgttableloadtype = tableinfo.split("~")[3]
            srctablequery = tableinfo.split("~")[4]

            srctablequery = srctablequery.replace(
                'v_currtimestamp', str(pendulum.now(timezone).strftime("%Y-%m-%d %H:%M:%S")))

            if src_server_name:
                srctablequery = srctablequery.replace('v_server_name', src_server_name)
                srctablename = srctablename.replace('v_server_name', src_server_name)

            if src_db_name:
                srctablequery = srctablequery.replace('v_db_name', src_db_name)
                srctablename = srctablename.replace('v_db_name', src_db_name)

            v_chunksize = int(tableinfo.split("~")[5])

            if src_db_type == 'db2':
                from_index=srctablequery.lower().index('from')
                from_query=' '
                for j in range(from_index,len(srctablequery)):
                    from_query=from_query+srctablequery[j]

                #srctablecountquerywhereclause = srctablequery.split(" where ")[1]
                srctablecountquery = "select count(*) as SRC_COUNT  " +from_query
            else: 
                group_by_pattern = re.compile("\s*group\s*by\s*", re.IGNORECASE)
                union_pattern = re.compile("\s*union\s*", re.IGNORECASE)
                distinct_pattern = re.compile("\s*distinct\s*", re.IGNORECASE)

                group_by_find = re.search(group_by_pattern, srctablequery)
                union_find = re.search(union_pattern, srctablequery)
                distinct_find = re.search(distinct_pattern, srctablequery)

                if group_by_find is None and union_find is None and distinct_find is None:
                    srctablecountquerywhereclause = re.split(" from ", srctablequery, maxsplit=1, flags=re.IGNORECASE)[1]
                    srctablecountquery = "select count(1) as SRC_COUNT FROM " + srctablecountquerywhereclause
                else:
                    if src_db_type == 'cache':
                        srctablecountquery = "select count(1) as SRC_COUNT FROM (" + srctablequery.strip(";") + ")"
                    else:
                        srctablecountquery = "select count(1) as SRC_COUNT FROM (" + srctablequery.strip(";") + ") count"

            logging.info("Count Query " + srctablecountquery)

            src_rec_count = pd.read_sql(srctablecountquery, conn)
            src_rec_count = src_rec_count['SRC_COUNT'][0]

            logging.info("===Starting process to  extract {} and load {} at {}===".format(
                srctablename, tgttablename, time.strftime("%Y%m%d-%H:%M:%S")))
            # some characters in column names need to be replaced
            col_spl_char = ['.', '[', ']']

            load_count = 0

            append_replace = 'append'

            # setup truncate/replace/append option
            if tgttableloadtype == 'replace':
                append_replace = 'replace'
                logging.info(
                    "===Drop and Recreate table {}===".format(tgttablename))

            elif tgttableloadtype == 'append':
                logging.info(
                    "===Create table {} if not exists===".format(tgttablename))

            elif tgttableloadtype == 'truncate':

                pd.read_gbq("truncate table " + tgttablename,
                            project_id=bqproject_id)
                logging.info(
                    "===Truncated table {}===".format(tgttablename))

            # Read source query using jdbc connection , rename columns if needed and write to bq table
            tableload_start_time = str(pendulum.now(timezone))[:23]

            try:

                bq_dataset = tgttablename.split('.')[0]
                tgt_bq_table = tgttablename.split('.')[1]
                bqclient = bigquery.Client(bqproject_id)
                dataset_ref = bqclient.dataset(bq_dataset)
                table_ref = dataset_ref.table(tgt_bq_table)
                table = bqclient.get_table(table_ref)
                # logging.info(table.schema)
                f = io.StringIO("")
                bqclient.schema_to_json(table.schema, f)
                tblschema = json.loads(f.getvalue())
                logging.info(tblschema)

                tgt_bq_table = bq_dataset + '.' + tgt_bq_table

                # job_config = bigquery.job.LoadJobConfig()
                # job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
                # job_config.schema = tblschema
                # job_config.allow_quoted_newlines = True
                
                # logging.info(srctablequery)
                logging.info("===Starting process to  extract {} and load {} at {}===".format(
                srctablequery, tgttablename, time.strftime("%Y%m%d-%H:%M:%S")))

                try:

                    for chunk in pd.read_sql(srctablequery, conn, chunksize=v_chunksize):
                        chunk.columns = map(str.lower, chunk.columns)
                        for char in col_spl_char:
                            chunk.columns = chunk.columns.str.replace(
                                char, '_', regex=False)

                        # logging.info(chunk.dtypes)
                        logging.info("dytype fixing begins")
                        # Change column name and datatype of dataframe
                        for i in range(0, len(tblschema)):
                            # logging.info(tblschema[i])
                            dtype = tblschema[i]['type'].lower()
                            
                            if dtype == 'time':
                                dtype = 'datetime64[ns]'
                            elif dtype == 'integer':
                                dtype = 'Int64'
                            elif dtype in ['numeric','bignumeric']:
                                dtype = 'float64'
                            elif dtype == 'string':
                                dtype = 'str'

                            # Converting float column to decimal to match numeric in BQ
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

                            elif i < len(chunk.columns):
                                chunk[tblschema[i]['name']] = chunk[tblschema[i]
                                                                    ['name']].astype(dtype)

                            else:
                                chunk[tblschema[i]['name']] = np.nan
                                chunk[tblschema[i]['name']] = chunk[tblschema[i]
                                                                    ['name']].astype(dtype)
                        
                        for attempt in range(1, 8):
                            try:
                                # logging.info(chunk.info())
                                pandas_gbq.to_gbq(chunk, tgt_bq_table,
                                            project_id=bqproject_id, if_exists='append', table_schema=tblschema)
                                
                                break
                                # load_job = bqclient.load_table_from_dataframe(chunk, table_ref, job_config=job_config)
                                # load_job.result() # waits for the load job to finish
                                # logging.info(f'Loaded {load_job.output_rows} rows in table {table}')
                            except pandas_gbq.exceptions.GenericGBQException as e:
                                logging.info("data load attempt {} failed with exception {}".format(attempt, str(e)))
                                if 'Exceeded rate limits' in str(e):
                                    wait_time = (math.ceil(2.5 ** attempt))
                                    time.sleep(wait_time)
                                else:
                                    logging.info("{} rows not loaded into table {} from server:{} db {}:".format(chunk.index,
                                        tgttablename,src_server_name,src_db_name ))                                
                        
                        load_count += len(chunk.index)

                except:
                    logging.info("===Unable to pull from db : {} rows loaded into table {}===".format(
                                src_db_name, tgttablename))
                    logging.info(traceback.format_exc())
                logging.info("==={} rows loaded into table {}===".format(
                    load_count, tgttablename))

            except:
                logging.info("===Unable to pull any more rows : {} rows loaded into table {}===".format(
                    load_count, tgttablename))
                logging.info(traceback.format_exc())
                if src_db_type == 'db2' : 
                    logging.info("Not Raising System Exit to handle com.ibm.db2.jcc.am.com.ibm.db2.jcc.am.SqlException \
                                 Invalid operation: result set  is closed. ERRORCODE=-4470, SQLSTATE=null")
                else :
                    raise SystemExit()

            tableload_end_time = str(pendulum.now(timezone))[:23]
            tableload_run_time = (pd.to_datetime(
                tableload_end_time) - pd.to_datetime(tableload_start_time))
            tgt_rec_count = load_count

            if src_rec_count == tgt_rec_count:
                audit_status = 'PASS'
            elif tgt_rec_count > src_rec_count:
                audit_status = 'PASS(More records in Target)'
            else:
                audit_status = 'FAIL'

            audit_insert_stmt = f"insert into {config['env']['v_audittablename']} values (GENERATE_UUID(), {srctableid}, '{sourcesysnm}', \
                '{srctablename}', '{tgttablename}', 'RECORD_COUNT',  {src_rec_count}, {tgt_rec_count}, '{tableload_start_time}', \
                '{tableload_end_time}', '{tableload_run_time}', '{jobname}', '{str(pendulum.now(timezone))[:23]}', '{audit_status}')"

            audit_entry = pd.read_gbq(
                audit_insert_stmt,  project_id=bqproject_id, max_results=0)
            logging.info(
                "===Audit entry added for srctableid {} - srctablename {} ===".format(srctableid, srctablename))

            logging.info(
                "===Execute Validation SQL's if any for table  {} ===".format(tgttablename))
            bqtable = tgttablename.split(".")[1]
            self.executevalidationsqls(bqtable)

            dt2 = dt.now()
            logging.info("Time Taken " + str(dt2-dt1))

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
        "--job_name", jobname.replace(".","-"),
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
        "--worker_machine_type", srcsys_config['v_machine_type'],
        "--setup_file", '{}setup.py'.format(utilpath)

    ]

    try:
        logging.info(
            "===Apache Beam Run with pipeline options started===")
        pcoll = beam.Pipeline(argv=pipeline_args)
        
        if beam_runner == 'DataflowRunner':
            pcoll | "Initialize" >> beam.Create(["1"]) | 'Setup Dataflow Worker' >> beam.ParDo(
                setuprunnerenv()) | "Read JDBC, Write to Google BigQuery" >> beam.ParDo(jdbctobq())
        else:
            pcoll | "Initialize.." >> beam.Create(
                ["1"]) | "Read JDBC, Write to Google BigQuery" >> beam.ParDo(jdbctobq())

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


def main(sourcesysnm, src_tbl_list):

    global jobname
    randomstring = ''.join(random.choices(
        string.ascii_lowercase + string.digits, k=8))
    jobname = sourcesysnm[:3] + "-staging-load-" + src_tbl_list + '-' + time.strftime("%Y%m%d%H%M%S") + '-' + randomstring
        
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
    parser.add_argument("--src_tbl_list", required=True,
                        help=("Source Table List"))
    parser.add_argument("--src_jdbc_lib_path", required=False,
                        help=("JDBC Lib Path"))
    parser.add_argument("--beam_runner", required=True,
                        help=("Beam Runner"))
   
    
    args = parser.parse_args()

    global srcsys_config_file
    srcsys_config_file = args.src_sys_config_file

    global srcsys_config
    srcsys_config = call_config_yaml(args.src_sys_config_file)

    global src_tbl_list
    src_tbl_list = args.src_tbl_list

    global sourcesysnm
    sourcesysnm = srcsys_config['v_sourcesysnm']

    global src_jdbc_lib_path
    src_jdbc_lib_path = args.src_jdbc_lib_path

    logging.info(src_jdbc_lib_path)

    global beam_runner
    beam_runner = args.beam_runner

    logging.info(beam_runner)

    logging.info("===BEGIN: Data Pipeline for src_tbl_list {}-{} at {}===".format(
        sourcesysnm, src_tbl_list, time.strftime("%Y%m%d-%H:%M:%S")))
    main(sourcesysnm, src_tbl_list)
    