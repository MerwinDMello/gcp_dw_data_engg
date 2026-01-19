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
import numpy as np
from sqlalchemy import create_engine


cwd = os.path.dirname(os.path.abspath(__file__))
print(cwd)
base_dir = os.path.dirname(cwd)
sys.path.insert(0, base_dir)
utilpath = base_dir + '/utils/'
config_folder = base_dir + "/config/"
#global validationsql_folder
#validationsql_folder = base_dir + "/sql/validation_sql/"


def call_config_yaml(filename, variablename):
    # load input config .yaml file from config folder
    cfgfile = open(config_folder + filename)
    default_config = yaml.safe_load(cfgfile)
    default_config = json.loads(json.dumps(default_config, default=str))
    config = default_config
    return config


config = call_config_yaml("hrg_config.yaml", "hca_hrg_default_vars")


def access_secret(secret_resourceid):
    #logging.info("Get Secret Version {}".format(secret_resourceid))
    client = secretmanager.SecretManagerServiceClient()
    payload = client.access_secret_version(
        secret_resourceid).payload.data.decode("UTF-8")
    return payload


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
        # logging.info(base_dir)

        return list("1")


class jdbctobq(beam.DoFn):

    def executevalidationsqls(self, bq_table):
        bq_table = bq_table.lower()

        if config['env']['v_runner'] == 'DataflowRunner':
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
                # logging.info(df)
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
        import decimal
        decimal_context = decimal.Context(prec=13)
        # logging.info(base_dir)

        # Override jaydebeapi package default converter to fix milliseconds processing bug
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
            jdbc_url = srcsys_config['v_jdbc_url']
            jdbc_jar = srcsys_config['v_jdbc_jar']

            username = srcsys_config['v_user']
            passwd = access_secret(
                config['env']['v_pwd_secrets_url'] + srcsys_config['v_pwd_secret_name'])

            # set jdbc lib path to /tmp/ on Dataflow runner
            if config['env']['v_runner'] == 'DataflowRunner':
                jdbc_lib_path = base_dir
            else:
                jdbc_lib_path = srcsys_config['v_jdbc_lib_path']
            # logging.info(jdbc_lib_path)
            conn = jaydebeapi.connect(jdbc_class_name, jdbc_url, {
                'user': username, 'password': passwd}, jdbc_lib_path + jdbc_jar, jdbc_lib_path)

            # read input table info and extract table/query details
            srctableid = tableinfo.split("~")[0]
            srctablename = tableinfo.split("~")[1]
            tgttablename = tableinfo.split("~")[2]

            # replace schema names in bq target table list with values from env config file
            tgttablename = tgttablename.replace(
                'v_hr_stage_dataset_name', config['env']['v_hr_stage_dataset_name'])
            tgttablename = tgttablename.replace(
                'v_hr_core_dataset_name', config['env']['v_hr_core_dataset_name'])

            tgttableloadtype = tableinfo.split("~")[3]
            srctablequery = tableinfo.split("~")[4]

            srctablequery = srctablequery.replace(
                'v_currtimestamp', str(pendulum.now(timezone).strftime("%Y-%m-%d %H:%M:%S")))
            
            if srctablename == 'lpahca.hrhistory':
                emp_history_last_date = pd.read_gbq(
                    f"select coalesce(max(last_update_date)-7,current_date()) as max_date from {config['env']['v_hr_core_dataset_name']}.hr_employee_history", project_id=config['env']['v_curated_project_id'])
                emp_history_last_date = list(emp_history_last_date['max_date'])[0]
                srctablequery = srctablequery.replace(
                    'v_emp_history_last_date', str(emp_history_last_date))


            if srctablename == 'lpahca.prtime':
                prtime_last_date = pd.read_gbq(
                    f"select coalesce(max(date_stamp),current_date()) as max_date from {config['env']['v_hr_stage_dataset_name']}.prtime", project_id=config['env']['v_curated_project_id'])
                prtime_last_date = list(prtime_last_date['max_date'])[0]
                srctablequery = srctablequery.replace(
                    'v_prtime_last_date', str(prtime_last_date))

            if src_db_name:
                srctablequery = srctablequery.replace('v_fs_db_name', src_db_name)
                srctablename = srctablename.replace('v_fs_db_name', src_db_name)

            logging.info(srctablequery)
            v_chunksize = int(tableinfo.split("~")[5])
            srctablecountquerywhereclause = srctablequery.split(" from ")[1]
            srctablecountquery = "select count(1) as SRC_COUNT from " + srctablecountquerywhereclause
            src_rec_count = pd.read_sql(srctablecountquery, conn)
            src_rec_count = src_rec_count['SRC_COUNT'][0]

            #control id changes
            src_rec_count_ctrl2 = 0
            src_rec_count_ctrl3 = 0 

            ctrl2_validation = tableinfo.split("~")[6]
            srctablecountquerywhereclause_ctrl2 = srctablequery.split(" from ")[1]
            srctablecountquery_ctrl2 = "select coalesce(" + ctrl2_validation + ",'0') as SRC_COUNT from " + \
            srctablecountquerywhereclause_ctrl2
            src_rec_count_ctrl2 = pd.read_sql(srctablecountquery_ctrl2, conn)
            src_rec_count_ctrl2 = int(decimal.Decimal(str(src_rec_count_ctrl2['SRC_COUNT'][0])))

            ctrl3_validation = tableinfo.split("~")[7]
            srctablecountquerywhereclause_ctrl3 = srctablequery.split(" from ")[1]
            srctablecountquery_ctrl3 = "select coalesce(" + ctrl3_validation + ",'0') as SRC_COUNT from " + \
            srctablecountquerywhereclause_ctrl3
            src_rec_count_ctrl3 = pd.read_sql(srctablecountquery_ctrl3, conn)
            src_rec_count_ctrl3 = int(decimal.Decimal(str(src_rec_count_ctrl3['SRC_COUNT'][0])))

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
            logging.info("creating sqlalchemy engine")
            # engine = create_engine(conn)

            try:

                bq_dataset = tgttablename.split('.')[0]
                tgt_bq_table = tgttablename.split('.')[1]
                bq_project_id = config['env']['v_curated_project_id']
                print(bq_project_id)
                bqclient = bigquery.Client(bq_project_id)
                print(bqclient)
                job_config = bigquery.job.LoadJobConfig()
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
                dataset_ref = bqclient.dataset(bq_dataset)
                table_ref = dataset_ref.table(tgt_bq_table)
                table = bqclient.get_table(table_ref)
                # logging.info(table.schema)
                f = io.StringIO("")
                bqclient.schema_to_json(table.schema, f)
                tblschema = json.loads(f.getvalue())
                # logging.info(tblschema)

                tgt_bq_table = bq_dataset + '.' + tgt_bq_table
                logging.info("sql reading begins")
                for chunk in pd.read_sql_query(srctablequery, conn, chunksize=v_chunksize):
                    logging.info("sql read")

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
                            dtype = 'int64'
                        elif dtype == 'numeric':
                            dtype = 'float64'
                            # logging.info(chunk[tblschema[i]['name']] )

                        # Converting float column to decimal to match numeric in BQ
                        if dtype == 'float64':
                             # logging.info("changing to float64")
                             chunk[tblschema[i]['name']] = chunk[tblschema[i]['name']].apply(dtype)
                             chunk[tblschema[i]['name']] = chunk[tblschema[i]['name']].apply(decimal_context.create_decimal_from_float)

                        #elif dtype == 'datetime':
                         #   chunk[tblschema[i]['name']] = pd.to_datetime(chunk[tblschema[i]['name']], infer_datetime_format=True)              
                        elif dtype == 'date':
                            # logging.info('fixing date')
                            chunk[tblschema[i]['name']] = chunk[tblschema[i]['name']].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d") if x is not None else np.nan)
                            # logging.info('fixing date successfull')
                        elif dtype == 'datetime':
                            # logging.info('fixing datetime')
                            chunk[tblschema[i]['name']] = chunk[tblschema[i]['name']].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S") if x is not None else np.nan)
                            # logging.info('fixing datetime successfull')
                        elif i < len(chunk.columns):
                            # logging.info('entered replacing step')
                            chunk[tblschema[i]['name']] = chunk[tblschema[i]
                                                                ['name']].astype(dtype)
                            # logging.info('exit replacing step')

                        else:
                            chunk[tblschema[i]['name']] = None
                            chunk[tblschema[i]['name']] = chunk[tblschema[i]
                                                                ['name']].astype(dtype)
                                      
                        
                    # logging.info(chunk.dtypes)
                    # logging.info(chunk.info())
                    logging.info("loading begins")
                    pandas_gbq.to_gbq(chunk, tgt_bq_table,
                                      project_id=bq_project_id, if_exists='append', table_schema=tblschema)

                    load_count += len(chunk.index)
                logging.info("==={} rows loaded into table {}===".format(
                    load_count, tgttablename))
                logging.info("sql reading loop begins begins")

            except:
                logging.info("===Unable to pull any more rows : {} rows loaded into table {}===".format(
                    load_count, tgttablename))
                logging.info(traceback.format_exc())
                # exc_type, exc_obj, exc_tb = sys.exc_info()
                # fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                # logging.info(exc_type, fname, exc_tb.tb_lineno)
                if srcsys_config['v_databasetype'] == 'db2' : 
                    logging.info("Not Raising System Exit to handle com.ibm.db2.jcc.am.com.ibm.db2.jcc.am.SqlException \
                                    Invalid operation: result set  is closed. ERRORCODE=-4470, SQLSTATE=null")
                    # if src_rec_count <= load_count:
                    #     logging.info("Not Raising System Exit to handle com.ibm.db2.jcc.am.com.ibm.db2.jcc.am.SqlException \
                    #                 Invalid operation: result set  is closed. ERRORCODE=-4470, SQLSTATE=null")
                    # else:
                    #     raise SystemExit()
                else :
                    raise SystemExit()


            tableload_end_time = str(pendulum.now(timezone))[:23]
            tableload_run_time = (pd.to_datetime(
                tableload_end_time) - pd.to_datetime(tableload_start_time))
            tgt_rec_count = load_count
            
            #control id changes
            tgt_count_ctrl2 = 0
            tgt_count_ctrl3 = 0
            ctrl2_validation = tableinfo.split("~")[8]
            tgt_count_ctrl2 = pd.read_gbq(
                    "select cast(coalesce(" + ctrl2_validation + ",0) as numeric) as tgt_count from " + tgttablename, project_id=bqproject_id)
            tgt_count_ctrl2 = int(tgt_count_ctrl2['tgt_count'][0])
            ctrl3_validation = tableinfo.split("~")[9]
            tgt_count_ctrl3 = pd.read_gbq(
                    "select cast(coalesce(" + ctrl3_validation + ",0) as numeric) as tgt_count from " + tgttablename, project_id=bqproject_id)
            tgt_count_ctrl3 = int(tgt_count_ctrl3['tgt_count'][0])

            # if src_rec_count > tgt_rec_count:
            #     audit_status = 'FAIL'
            if (src_rec_count < tgt_rec_count) :
                audit_status = 'PASS(More records in Target)'
            elif (src_rec_count == tgt_rec_count and src_rec_count_ctrl2 == tgt_count_ctrl2 and src_rec_count_ctrl3 == tgt_count_ctrl3) :
                audit_status = 'PASS'
            else:
                audit_status = 'FAIL'

            # tgt_count = pd.read_gbq(
            #     "select count(1) as tgt_count from " + tgttablename, project_id=bqproject_id)
            # tgt_count = tgt_count['tgt_count'][0]
            audit_insert_stt = "insert into {} values (GENERATE_UUID() , {}, '{}', '{}', '{}', '{}',  {}, {}, {}, {},{}, {}, '{}', '{}', '{}', '{}', '{}', '{}'  ) " .format(
                config['env']['v_audittablename_lawson'],
                srctableid,
                sourcesysnm,
                srctablename,
                tgttablename,
                'RECORD_COUNT',
                src_rec_count,
                tgt_rec_count,
                src_rec_count_ctrl2,
                tgt_count_ctrl2,
                src_rec_count_ctrl3,
                tgt_count_ctrl3,
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

            logging.info(
                "===Execute Validation SQL's if any for table  {} ===".format(tgttablename))
            bqtable = tgttablename.split(".")[1]
            self.executevalidationsqls(bqtable)

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

        logging.info( "===Submitting Asynchronous Dataflow Job===")
        p = pcoll.run()
        dataflow_job_id = p.job_id()
        logging.info( "===Submitted Asynchronous Dataflow Job, Job id is " + dataflow_job_id + " ====")
        return dataflow_job_id


    except:
        logging.exception("===ERROR: Apache Beam Run Failed===")
        raise SystemExit()


def main(sourcesysnm, src_tbl_list):

    global jobname
    randomstring = ''.join(random.choices(
        string.ascii_lowercase + string.digits, k=8))
    if src_db_name:
        jobname = sourcesysnm[:3] + "-p-" + src_db_name.lower() + '-' + src_tbl_list + '-' + time.strftime("%Y%m%d%H%M%S") + '-' + randomstring        
    else: 
        jobname = sourcesysnm[:3] + "-p-" + srcsys_config_file[:-12].replace(
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
    parser.add_argument("--src_db_name", required=False,
                        help=("Source Database Name"))
    args = parser.parse_args()

    global srcsys_config_file
    srcsys_config_file = args.src_sys_config_file

    global srcsys_config
    srcsys_config = call_config_yaml(
        args.src_sys_config_file, args.src_sys_airflow_varname)

    global src_tbl_list
    src_tbl_list = args.src_tbl_list

    global src_db_name
    src_db_name = args.src_db_name

    global sourcesysnm
    sourcesysnm = srcsys_config['v_sourcesysnm']

    logging.info("===BEGIN: Data Pipeline for src_tbl_list {}-{} at {}===".format(
        sourcesysnm, src_tbl_list, time.strftime("%Y%m%d-%H:%M:%S")))
    main(sourcesysnm, src_tbl_list)