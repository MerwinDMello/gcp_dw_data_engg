import logging
import os
import decimal
import traceback
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
import time
import pendulum
import numpy as np

timezone = pendulum.timezone("US/Central")

cwd = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.dirname(cwd)
sys.path.insert(0, base_dir)
utilpath = base_dir + '/utils/'
config_folder = base_dir + "/config/"


# global validationsql_folder
# validationsql_folder = base_dir + "/sql/validation_sql/"


def call_config_yaml(filename, variablename):
    # load input config .yaml file from config folder
    cfgfile = open(config_folder + filename)
    default_config = yaml.safe_load(cfgfile)
    default_config = json.loads(json.dumps(default_config, default=str))
    config = default_config
    return config


config = call_config_yaml("ra_config.yaml","hca_ra_default_vars")

def access_secret(secret_resourceid):
    # logging.info("Get Secret Version {}".format(secret_resourceid))
    client = secretmanager.SecretManagerServiceClient()
    payload = client.access_secret_version(name=secret_resourceid).payload.data.decode("UTF-8")
    return payload


class setuprunnerenv(beam.DoFn):
    def process(self, context):
        # jdkfile = ra_oracle['v_jdkfile']
        # gcsjarbucket = config['env']['v_dfjarbucket']
        # jdkversion = ra_oracle['v_jdkversion']
        # jdbcjar = ra_oracle['v_jdbc_jar']
        #
        # # use /tmp/ on dataflow worker node for processing
        global base_dir
        base_dir = '/tmp/'
        # os.system('gsutil cp ' + gcsjarbucket +
        #           jdbcjar + ' ' + base_dir + ' && ls ')
        #
        # # Copy required java libraries
        # os.system('gsutil cp ' + gcsjarbucket +
        #           jdkfile + ' ' + base_dir + ' && ls ')
        #
        # # setup jvm path and java version
        # os.system('mkdir -p /usr/lib/jvm')
        # os.system('tar xvzf ' + base_dir + jdkfile + ' -C /usr/lib/jvm')
        # os.system(
        #     'update-alternatives --install "/usr/bin/java" "java" "/usr/lib/jvm/' + jdkversion + '/bin/java" 1 ')
        # os.system('update-alternatives --config java')
        # logging.info('JDK Libraries copied to Instance..')

        os.system('java -version')

        logging.info("Displaying tmp files")
        os.listdir('/tmp/')

        return list("1")


class jdbctobq(beam.DoFn):

    def executevalidationsqls(self, bq_table):
        bq_table = bq_table.lower()

        if config['env']['v_runner'] == 'DataflowRunner':
            sqlbucket = 'gs://' + \
                        config['env']['v_dag_bucket_name'] + \
                        '/dags/edwra/sql/validation_sql/'
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
                df = pd.read_gbq(bqsqlsqry, project_id=bqproject_id)
                logging.info(df)
        else:
            logging.info(
                "===Did not find any Validation SQL for table - {}===".format(bq_table))

    def readjdbcwritebqtable(self, tableinfo, bqproject_id):
        import jaydebeapi
        import oracledb
        import pendulum
        import pandas_gbq
        import io
        from jaydebeapi import _DEFAULT_CONVERTERS
        from datetime import datetime
        import decimal
        decimal_context = decimal.Context(prec=13)

        # Override jaydebeapi package default converter to fix milliseconds processing bug
        def _to_datetime(rs, col):

            java_val = rs.getTimestamp(col)
            if not java_val:
                return
            d = datetime.strptime(str(java_val)[:19], "%Y-%m-%d %H:%M:%S")
            d = d.replace(microsecond=java_val.getNanos() // 1000)
            return str(d)

        _DEFAULT_CONVERTERS.update({"TIMESTAMP": _to_datetime})

        def parse_timestamp(timestamp_str):
            try:
                return datetime.strptime(timestamp_str,'%Y-%m-%d %H:%M:%S')
            except ValueError:
                return datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S.%f').replace(microsecond=0)

        try:

            timezone = pendulum.timezone("US/Central")
            # set jdbc lib path to /tmp/ on Dataflow runner
            # if config['env']['v_runner'] == 'DataflowRunner':
            #     jdbc_lib_path = base_dir
            # else:
            #     jdbc_lib_path = clinical['v_jdbc_lib_path']

            logging.info(f"Source schema id is: {src_schema_id}")

            if src_schema_id == '1':
                passwd = access_secret(config['env']['v_pwd_secrets_url'] + ra_oracle['v_pwd_secret_name_p1'])
                service_name = ra_oracle['servicename_p1']

            elif src_schema_id == '3':
                passwd = access_secret(config['env']['v_pwd_secrets_url'] + ra_oracle['v_pwd_secret_name_p2'])
                service_name = ra_oracle['servicename_p2']    
            
            user = ra_oracle['v_user']
            host_name = ra_oracle['hostname']
            
            conn_str='{}/{}@{}/{}'.format(user,passwd,host_name,service_name)

            conn = oracledb.connect(conn_str) 
            test_query = 'SELECT * FROM CONCUITY.CE_Service WHERE ROWNUM <= 10'
            df_ora = pd.read_sql(test_query, con=conn)
            # logging.info(df_ora.head())

            # read input table info and extract table/query details
            srctableid = tableinfo.split("~")[0]
            srctablename = tableinfo.split("~")[1]
            tgttablename = tableinfo.split("~")[2]
            tgttableloadtype = tableinfo.split("~")[3]
            srctablequery = tableinfo.split("~")[4]
            srctablequery = srctablequery.replace('$SchemaID', src_schema_id)
            v_chunksize = int(tableinfo.split("~")[5])
            tgttablename = tgttablename.replace('v_parallon_ra_stage_dataset_name', config['env']['v_parallon_ra_stage_dataset_name'])

            # tgttablenamecdc = tableinfo.split("~")[3]
            # tgttablenamecdc = tgttablenamecdc.replace('v_parallon_ra_stage_dataset_name', config['env']['v_parallon_ra_stage_dataset_name'])
            
            is_cdc = False
            # TODO: Query CDC_IND table for potential cdc jobs
            # is_cdc = tgttablenamecdc != ''
            # tgttableloadtype = tableinfo.split("~")[3]
            #primary_keys = tableinfo.split("~")[5]
            #if primary_keys != '':
            #    primary_keys = [k.strip() for k in primary_keys.split(',')]
            # srctablequery = tableinfo.split("~")[4]
            # # srctablequery = srctablequery.replace('v_curr_timestamp', str(pendulum.now(timezone))[:23])
            # srctablequery = srctablequery.replace('$SchemaID', src_schema_id)
            # # srctablequeryfull = tableinfo.split("~")[7]
            # # srctablequeryfull = srctablequeryfull.replace('v_curr_timestamp', str(pendulum.now(timezone))[:23])
            # # srctablequeryfull = srctablequeryfull.replace('$SchemaID', src_schema_id)
            # v_chunksize = int(tableinfo.split("~")[5])

            logging.info("===Starting process to  extract {} and load {} at {}===".format(
                srctablename, tgttablename, time.strftime("%Y%m%d-%H:%M:%S")))
            # some characters in column names need to be replaced
            col_spl_char = ['.', '[', ']']

            load_count = 0
            count_query="select count(*) as count from ({}) a".format(srctablequery)
            src_rec_count = pd.read_sql(count_query, con=conn)['COUNT'].tolist()[0]
            logging.info("Count for table {} from source is {}".format(srctablename,src_rec_count))

            if tgttableloadtype.lower() == 'delete':
                if is_cdc:
                    logging.info(
                        "===Deleting rows from CDC table {} with srcSchema ={}===".format(tgttablenamecdc, src_schema_id))
                    pd.read_gbq("DELETE FROM {} WHERE Schema_Id = {};".format(tgttablenamecdc, src_schema_id) ,
                                project_id=bqproject_id)
                    logging.info(
                        "===Inserting rows from table {} into CDC table {} with srcSchema ={}===".format(tgttablename, tgttablenamecdc, src_schema_id))
                    pd.read_gbq("INSERT INTO {} SELECT * FROM {} WHERE Schema_Id = {};".format(tgttablenamecdc, tgttablename, src_schema_id) ,
                                project_id=bqproject_id)
                    
                elif tgttablename.lower() == 'edwra_staging.cc_remit_recon_aggregated':
                    logging.info(
                        "===Deleting rows from table {} WHERE DATE(dw_last_update_date_time) = current_date() - 54===".format(tgttablename))
                    pd.read_gbq("DELETE FROM {} WHERE DATE(dw_last_update_date_time) = current_date() - 54;".format(tgttablename) ,
                                project_id=bqproject_id)
                    
                elif tgttablename.lower() == 'edwra_staging.mon_acct_actvty_rcn_pass1' or tgttablename.lower() == 'edwra_staging.gl_report_year_temp':
                    logging.info(
                        "===Deleting rows from table {} with srcSchema = {}===".format(tgttablename, src_schema_id))
                    pd.read_gbq("DELETE FROM {} WHERE Schema_Id = '{}';".format(tgttablename, src_schema_id) ,
                            project_id=bqproject_id)
                    
                else:
                    logging.info(
                        "===Deleting rows from table {} with srcSchema ={}===".format(tgttablename, src_schema_id))
                    pd.read_gbq("DELETE FROM {} WHERE Schema_Id = {};".format(tgttablename, src_schema_id) ,
                                project_id=bqproject_id)
                    
            elif tgttableloadtype.lower() == 'truncate':
                logging.info("TRUNCATE TABLE {} ;".format(tgttablename))
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

                # if sourcesysnm == 'mhb':
                #     logging.info("Deleting records in mhb table before inserting to avoid duplication in case of job fails and restarts")
                #     delete_query = "delete from {0}.{1} where lower(databasename)=lower('{2}')".format(bqproject_id, tgt_bq_table.lower(), srctablename.split('.')[0])
                #     if tgt_bq_table.lower() == 'edwci_staging.vw_patient_vital_audits_stg':
                #         delete_query = "delete from {0}.{1} where lower(database_name)=lower('{2}')".format(bqproject_id,tgt_bq_table.lower(),srctablename.split('.')[0])
                #
                #     logging.info("=======query used to delete data is :  {}".format(delete_query))
                #     delete_value = pd.read_gbq(delete_query, project_id=bqproject_id)

                logging.info("===query being used is : {}===".format(srctablequery))

                for chunk in pd.read_sql(srctablequery, conn, chunksize=v_chunksize):
                    chunk.columns = map(str.lower, chunk.columns)
                    for char in col_spl_char:
                        chunk.columns = chunk.columns.str.replace(char, '_', regex=False)

                    # logging.info(chunk.dtypes)

                    # Change column name and datatype of dataframe
                    logging.info("===schema for table {} is : {}===".format(tgttablename,table.schema))
                    for x in table.schema:
                        dtype = x.field_type.lower()
                        if dtype == 'numeric':
                            #dtype = 'float64'
                            #chunk[x.name]=chunk[x.name].astype(str).map(decimal.Decimal)
                            chunk[x.name]=chunk[x.name].apply('float64')
                            chunk[x.name]=chunk[x.name].apply(decimal_context.create_decimal_from_float)
                        elif dtype == 'date' or dtype == 'time' or dtype == 'datetime':
                            #dtype = 'datetime64[ns]'
                            chunk[x.name] = chunk[x.name].astype('datetime64[ns]')
                        elif dtype == 'integer':
                            #dtype = 'int64'
                            chunk[x.name] = chunk[x.name].astype('Int64')
                        elif dtype == 'float':
                            chunk[x.name]=chunk[x.name].astype('float64')
                        else:
                            #dtype = 'str'
                            chunk[x.name].fillna("nan",inplace=True)
                            chunk[x.name] = chunk[x.name].astype('str')

                        #chunk[x.name] = chunk[x.name].astype(dtype)

                    # logging.info(chunk.info())
                    chunk.replace(['nan'], None, inplace=True)
                    pandas_gbq.to_gbq(chunk, tgt_bq_table,project_id=bq_project_id, if_exists='append', table_schema=tblschema)

                    load_count += len(chunk.index)
                logging.info("==={} rows loaded into table {}===".format(
                    load_count, tgttablename))
                conn.close()

            except:
                logging.info("===Unable to pull any more rows : {} rows loaded into table {}===".format(
                    load_count, tgttablename))
                logging.info(traceback.format_exc())
                if ra_oracle['v_databasetype'] == 'db2':
                    logging.info("Not Raising System Exit to handle com.ibm.db2.jcc.am.com.ibm.db2.jcc.am.SqlException \
                                 Invalid operation: result set  is closed. ERRORCODE=-4470, SQLSTATE=null")
                else:
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

            # tgt_count = pd.read_gbq(
            #     "select count(1) as tgt_count from " + tgttablename, project_id=bqproject_id)
            # tgt_count = tgt_count['tgt_count'][0]
            if sourcesysnm == 'ra':
                audit_insert_stt = "insert into {} values (GENERATE_UUID() , {}, '{}', '{}', '{}', '{}', {}, {}, '{}', '{}', '{}', '{}', '{}', '{}'  ) ".format(
                    config['env']['v_parallon_ra_audit_dataset_name']+"."+ra_oracle['v_audittablename'],
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
            else:
                audit_insert_stt = "insert into {} values (GENERATE_UUID() , {}, '{}', '{}', '{}', '{}', {}, {}, {}, {}, {}, {}, '{}', '{}', '{}', '{}', '{}', '{}'  ) ".format(
                    config['env']['v_parallon_ra_audit_dataset_name']+"."+ra_oracle['v_audittablename'],
                    srctableid,
                    sourcesysnm,
                    srctablename,
                    tgttablename,
                    'RECORD_COUNT',
                    src_rec_count,
                    tgt_rec_count,
                    0,
                    0,
                    0,
                    0,
                    tableload_start_time,
                    tableload_end_time,
                    tableload_run_time,
                    jobname,
                    str(pendulum.now(timezone))[:23],
                    audit_status

                )

            audit_entry = pd.read_gbq(
                audit_insert_stt, project_id=bqproject_id, max_results=0)
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
            tablelist = ra_oracle[src_tbl_list]

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
        "--service_account_email", config['env']['v_df_atos_serviceaccountemail'],
        "--job_name", jobname,
        "--runner", config['env']['v_runner'],
        "--network", config['env']["v_network"],
        "--subnetwork", config['env']["v_subnetwork"],
        "--staging_location", config['env']["v_dfstagebucket"],
        "--temp_location", config['env']["v_gcs_temp_bucket"],
        "--region", config['env']["v_region"],
        "--save_main_session",
        "--num_workers", str(config['env']["v_numworkers"]),
        "--max_num_workers", str(config['env']["v_maxworkers"]),
        "--no_use_public_ips",
        "--dataflow_service_options, enable_prime",
        "--autoscaling_algorithm", "THROUGHPUT_BASED",
        "--worker_machine_type", ra_oracle['v_machine_type'],
        "--setup_file", '{}setup.py'.format(utilpath)
    ]


    try:
        logging.info(
            "===Apache Beam Run with pipeline options started===")
        pcoll = beam.Pipeline(argv=pipeline_args)
        if config['env']['v_runner'] == 'DataflowRunner':
            pcoll | "Initialize" >> beam.Create(["1"]) | 'Setup Dataflow Worker' >> beam.ParDo(
                setuprunnerenv()) | "Read JDBC, Write to Google BigQuery" >> beam.ParDo(jdbctobq())
        else:
            pcoll | "Initialize.." >> beam.Create(
                ["1"]) | "Read JDBC, Write to Google BigQuery" >> beam.ParDo(jdbctobq())

        logging.info("===Submitting Asynchronous Dataflow Job===")
        p = pcoll.run()
        dataflow_job_id = p.job_id()
        logging.info("===Submitted Asynchronous Dataflow Job, Job id is " + dataflow_job_id + " ====")
        return dataflow_job_id

    except:
        logging.exception("===ERROR: Apache Beam Run Failed===")
        raise SystemExit()


def main(sourcesysnm, src_tbl_list):
    global jobname
    randomstring = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
    if src_schema_id == '1':
        schema = 'p1'
    elif src_schema_id == '3':
        schema = 'p2'
    if src_db_name:
        jobname = sourcesysnm + "-p-" + src_db_name.replace('_', '-').lower() + '-' + src_tbl_list + '-' + schema + '-' + time.strftime("%Y%m%d%H%M%S") #+ '-' + randomstring
    else:
        jobname = sourcesysnm + "-p-" + srcsys_config_file.split('.')[0].replace('_', '-') + '-' + src_tbl_list + '-' + schema + '-' + time.strftime("%Y%m%d%H%M%S") #+ '-' + randomstring
        #jobname = sourcesysnm + "-p-" + srcsys_config_file.split('.')[0].replace('_','-') + '-' + time.strftime("%Y%m%d%H%M%S")  # + '-' + randomstring

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
    parser.add_argument("--src_schema_id", required=True, 
                        help=("Oracle Instance ID"))
    parser.add_argument("--src_db_name", required=False,
                        help=("Source Database Name"))
    args = parser.parse_args()

    global srcsys_config_file
    srcsys_config_file = args.src_sys_config_file

    global ra_oracle
    ra_oracle = call_config_yaml(args.src_sys_config_file, args.src_sys_airflow_varname)

    global src_tbl_list
    src_tbl_list = args.src_tbl_list

    global src_schema_id
    src_schema_id = args.src_schema_id

    global src_db_name
    src_db_name = args.src_db_name

    global sourcesysnm
    sourcesysnm = ra_oracle['v_sourcesysnm'].replace('_','-')

    logging.info("===BEGIN: Data Pipeline for src_tbl_list {}-{} at {}===".format(
        sourcesysnm, src_tbl_list, time.strftime("%Y%m%d-%H:%M:%S")))
    main(sourcesysnm, src_tbl_list)
