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
from google.auth.transport.requests import AuthorizedSession
import google.auth
import json
import time
import pendulum

timezone = pendulum.timezone("US/Central")

cwd = os.path.dirname(os.path.abspath(__file__))
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


config = call_config_yaml("hrg_config.yaml", "hrg_default_vars")


def access_secret(secret_resourceid):
    #logging.info("Get Secret Version {}".format(secret_resourceid))
    client = secretmanager.SecretManagerServiceClient()
    payload = client.access_secret_version(
        secret_resourceid).payload.data.decode("UTF-8")
    return payload


def get_dataflow_currentjobstate(project_id, location, jobid):
    dfbase_url = 'https://dataflow.googleapis.com/v1b3/projects/'
    credentials, project_id = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
    authed_session = AuthorizedSession(credentials)
    response = authed_session.request('GET', f'{dfbase_url}{project_id}/locations/{location}/jobs/{jobid}')
    #print(json.dumps(response.json(), indent=4))
    jobstatus = response.json()['currentState']
    return jobstatus


class setuprunnerenv(beam.DoFn):
    def process(self, context):
        global base_dir
        base_dir = '/tmp/'

        # for ats_infor save the ionapi file in the folder containing the Compass JDBC driver JAR file.
        if sourcesysnm == 'ats_infor':
            logging.info(
                'Copying Infor Compass JDBC Driver.ionapi file to /tmp/')
            inforcredfile = access_secret(
                config['env']['v_pwd_secrets_url'] + srcsys_config['v_ats_infor_secret_name'])
            ionapifile = open(
                base_dir + srcsys_config['v_infor_api_file_name'], 'w')
            ionapifile.write(inforcredfile)

        jdkfile = srcsys_config['v_jdkfile']
        gcsjarbucket = config['env']['v_dfjarbucket']
        jdkversion = srcsys_config['v_jdkversion']
        jdbcjar = srcsys_config['v_jdbc_jar']
        
        # use /tmp/ on dataflow worker node for processing

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
                bqsqlsqryfile = open(base_dir + filename)
                bqsqlsqry = bqsqlsqryfile.read()
                logging.info("===Executing SQL : {}===".format(filename))
                df = pd.read_gbq(bqsqlsqry,  project_id=bqproject_id)
                logging.info(df)
        else:
            logging.info(
                "===Did not find any Validation SQL for table - {}===".format(bq_table))

    def readjdbcwritebqtable(self, tableinfo, bqproject_id):
        import jaydebeapi
        import pandas_gbq
        import io

        try:
            #timezone = pendulum.timezone("Asia/Calcutta")

            jdbc_class_name = srcsys_config['v_jdbc_class_name']
            jdbc_url = srcsys_config['v_jdbc_url']
            jdbc_jar = srcsys_config['v_jdbc_jar']

            # for ats_infor no need of user/pwd
            if sourcesysnm == 'ats_infor':
                username = ''
                passwd = ''
            else:
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
                'v_hr_stage_dataset_name', config['env']['v_hr_stage_dataset_name'])
            tgttablename = tgttablename.replace(
                'v_hr_core_dataset_name', config['env']['v_hr_core_dataset_name'])

            tgttableloadtype = tableinfo.split("~")[3]
            srctablequery = tableinfo.split("~")[4]

            srctablequery = srctablequery.replace(
                'v_currtimestamp', str(pendulum.now(timezone))[:23])

            v_chunksize = int(tableinfo.split("~")[5])

            if sourcesysnm == 'ats_infor' and srctablename == 'cust_v2_resourcetransition':
                srctablecountquery = "select count(1) as SRC_COUNT from (select infor.dataobjectseqid(), infor.lastmodified() ilm, h.* from cust_v2_resourcetransition h, ( select max(infor.dataobjectseqid()) seqid, HROrganization, ResourceTransition from cust_v2_resourcetransition group by HROrganization, ResourceTransition) h2 where h.HROrganization = h2.HROrganization and h.ResourceTransition = h2.ResourceTransition and infor.dataobjectseqid() = h2.seqid)"
            else: 
                srctablecountquerywhereclause = srctablequery.split(" where ")[1]            
                srctablecountquery = "select count(1) as SRC_COUNT from " + \
                srctablename + " where " + srctablecountquerywhereclause
            
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
                bq_project_id = config['env']['v_curated_project_id']
                bqclient = bigquery.Client(bq_project_id)
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

                for chunk in pd.read_sql(srctablequery, conn, chunksize=v_chunksize):
                    chunk.columns = map(str.lower, chunk.columns)
                    for char in col_spl_char:
                        chunk.columns = chunk.columns.str.replace(
                            char, '_', regex=False)

                    # logging.info(chunk.dtypes)

                    # Change column name and datatype of dataframe
                    for i in range(0, len(tblschema)):
                        # logging.info(tblschema[i])
                        dtype = tblschema[i]['type'].lower()

                        if dtype == 'numeric':
                            dtype = 'object'
                        elif dtype == 'date' or dtype == 'time' or dtype == 'datetime':
                            dtype = 'datetime64'
                        elif dtype == 'integer':
                            dtype = 'Int64'

                        if i < len(chunk.columns):
                            chunk[tblschema[i]['name']] = chunk[tblschema[i]
                                                                ['name']].astype(dtype)

                        else:
                            chunk[tblschema[i]['name']] = None
                            chunk[tblschema[i]['name']] = chunk[tblschema[i]
                                                                ['name']].astype(dtype)
    
                    pandas_gbq.to_gbq(chunk, tgt_bq_table,
                                      project_id=bq_project_id, if_exists='append', table_schema=tblschema)

                    load_count += len(chunk.index)
                    logging.info("==={} rows loaded into table {}===".format(
                    load_count, tgttablename))

            except:
                # added this to handle db2 (landmark Invalid operation: result set is closed Error)
                logging.info("===Unable to pull any more rows : {} rows loaded into table {}===".format(
                    load_count, tgttablename))
                logging.info(traceback.format_exc())
                if srcsys_config['v_databasetype'] == 'db2' : 
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

            audit_insert_stt = "insert into {} values (GENERATE_UUID() , {}, '{}', '{}', '{}', '{}',  {}, {}, '{}', '{}', '{}', '{}', '{}', '{}'  ) " .format(
                config['env']['v_audittablename'],
                srctableid,
                sourcesysnm,
                srctablename,
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

        logging.info( "===Submitting Asynchronous Dataflow Job===")
        p = pcoll.run()
        jobid = p.job_id()
        logging.info( "===Submitted Asynchronous Dataflow Job, Job id is " + jobid + " ====")

        jobstatus ='JOB_STATE_SUBMITTED'
        while jobstatus != 'JOB_STATE_DONE' and jobstatus != 'JOB_STATE_CANCELLED' and jobstatus != 'JOB_STATE_FAILED':
            logging.info("===Status of Python Dataflow job is " + jobstatus  + " , sleeping for 30sec - " + str(pendulum.now(timezone)) + "===" )
            time.sleep(30)
            jobstatus = get_dataflow_currentjobstate(config['env']['v_proc_project_id'], config['env']['v_region'], jobid)        


        logging.info( "===Status of Python Dataflow job is " + jobstatus  + "===")
        if jobstatus != 'JOB_STATE_DONE' :             
            raise SystemExit()
       

    except:
        logging.exception("===ERROR: Apache Beam Run Failed===")
        raise SystemExit()


def main(sourcesysnm, src_tbl_list):

    global jobname
    randomstring = ''.join(random.choices(
        string.ascii_lowercase + string.digits, k=8))
    jobname = sourcesysnm[:3] + "-p-" + srcsys_config_file[:-5].replace(
        '_', '-') + '-' + src_tbl_list + '-' + time.strftime("%Y%m%d%H%M%S") + '-' + randomstring
    logging.info("===Job Name is {} ===".format(jobname))
    p = run()


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
    args = parser.parse_args()

    global srcsys_config_file
    srcsys_config_file = args.src_sys_config_file

    global srcsys_config
    srcsys_config = call_config_yaml(
        args.src_sys_config_file, args.src_sys_airflow_varname)

    global src_tbl_list
    src_tbl_list = args.src_tbl_list

    global sourcesysnm
    sourcesysnm = srcsys_config['v_sourcesysnm']

    logging.info("===BEGIN: Data Pipeline for src_tbl_list {}-{} at {}===".format(
        sourcesysnm, src_tbl_list, time.strftime("%Y%m%d-%H:%M:%S")))
    main(sourcesysnm, src_tbl_list)
    logging.info("===END: Data Pipeline for src_tbl_list {}-{} at {}===".format(
        sourcesysnm, src_tbl_list, time.strftime("%Y%m%d-%H:%M:%S")))
