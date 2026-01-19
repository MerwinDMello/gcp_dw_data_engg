import logging
import os
import sys
import traceback
import argparse
from datetime import datetime as dt
import pendulum
import time
import apache_beam as beam
import pandas as pd
import yaml
import json
import string
import random
from google.cloud import secretmanager
from google.cloud import storage

cwd = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.dirname(cwd)
sys.path.insert(0, base_dir)
utilpath = base_dir + '/utils/'
config_folder = base_dir + "/config/"

def call_config_yaml(filename):
    # load input config .yaml file from config folder
    cfgfile = open(config_folder + filename)
    default_config = yaml.safe_load(cfgfile)
    default_config = json.loads(json.dumps(default_config, default=str))
    config = default_config
    return config

config = call_config_yaml("edwclm_config.yaml")

current_timezone = pendulum.timezone("US/Central")

def access_secret(secret_resourceid):
    #logging.info("Get Secret Version {}".format(secret_resourceid))
    client = secretmanager.SecretManagerServiceClient()
    print(secret_resourceid)
    payload = client.access_secret_version(
    name=secret_resourceid).payload.data.decode("UTF-8")
    return payload

def qualify_done_file_pattern(polling_config_item):
    done_file_pattern = str(polling_config_item['done_file'])
    logging.info("Done File Pattern : {}".format(done_file_pattern))
            
    if polling_config_item['mask_info']:
        pattern_type = str(polling_config_item['mask_info']['type']).lower()
        logging.info("Mask Type : {}".format(pattern_type))
        match pattern_type:
            case 'current_date':
                datetime_value = pendulum.now(current_timezone)
            case 'previous_weekday':
                datetime_value = pendulum.now(current_timezone)
                day_of_week = int(datetime_value.strftime('%w'))
                if day_of_week == 1:
                    date_diff = 3
                else:
                    date_diff = 1
                datetime_value = datetime_value - timedelta(days=date_diff)

        pattern_mask_code = str(polling_config_item['mask_info']['code']).upper()
        logging.info("Mask Code : {}".format(pattern_mask_code))
        match pattern_mask_code:
            case 'YYYYMMDD':
                datetime_value_fmt = datetime_value.strftime("%Y%m%d")
            case 'DD-MMM-YYYY':
                datetime_value_fmt = datetime_value.strftime("%d-%b-%Y")
            case 'DD-MM-YYYY':
                datetime_value_fmt = datetime_value.strftime("%d-%m-%Y")

        formatted_done_file_pattern = done_file_pattern.replace(pattern_mask_code, datetime_value_fmt)
    else:
        formatted_done_file_pattern = done_file_pattern
    logging.info("Formatted Done File Pattern : {}".format(formatted_done_file_pattern))
    return done_file_pattern, formatted_done_file_pattern

def get_bucket(bq_project_id, bucket_name):
    # Get name of bucket
    try:
        storage_client = storage.Client(bq_project_id)
        bucket = storage_client.bucket(bucket_name)
        return bucket
    except:
        logging.info("Bucket {} is not found in project {}".format(bucket_name, bq_project_id))
        raise SystemExit()

def get_blob(bucket, objectfullpath):
    # Get full path of the file
    try:
        
        blob = bucket.blob(objectfullpath)
        logging.info(f"Found Object {objectfullpath}")
        return blob
    except:
        logging.info("Blob is not found in path {}".format(objectfullpath))
        raise SystemExit()

def upload_object_from_file(bq_project_id, gcs_bucket, objectfullpath, filepath):
    # Load file from local runtime directory to gcs bucket
    logging.info("==== In upload_object_from_file ====")
    logging.info("Project : {}, GCS Bucket : {}, Object Path : {}, File Path : {}".format(
        bq_project_id, gcs_bucket, objectfullpath, filepath))

    bucket = get_bucket(bq_project_id, gcs_bucket)
    blob = get_blob(bucket, objectfullpath)
    with open(filepath,"w") as f:
        f.write(" ")
    blob.upload_from_filename(filepath)
    logging.info("Object {} has been loaded using file {}".format(blob.name, filepath))
    return blob.name

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


    def replacevarwithliterals(self, src_table_stmt, src_server_name, src_db_name, process_status, process_name, curr_date):

        src_table_stmt = src_table_stmt.replace('v_curr_date', curr_date)

        if src_server_name:
            src_table_stmt = src_table_stmt.replace('v_server_name', src_server_name)

        if src_db_name:
            src_table_stmt = src_table_stmt.replace('v_db_name', src_db_name)

        if process_status:
            src_table_stmt = src_table_stmt.replace('v_process_status', process_status)

        if process_name:
            src_table_stmt = src_table_stmt.replace('v_process', process_name)

        return src_table_stmt

    def readjdbcwritebqtable(self, bqproject_id):
        import jaydebeapi
        try:

            dt1 = dt.now()

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

            curr_date = str(pendulum.now(current_timezone).strftime("%Y-%m-%d"))

            src_table_stmt = srcsys_config[src_db_select_sql]

            src_table_stmt = self.replacevarwithliterals(src_table_stmt, src_server_name, src_db_name, process_status, process_name, curr_date)

            # Read source query using jdbc connection , rename columns if needed and write to bq table
            polling_start_time = str(pendulum.now(current_timezone).strftime("%Y-%m-%d %H:%M:%S"))

            logging.info("===Starting process to  execute {} at {}===".format(
            src_table_stmt, polling_start_time))


            polling_record_found = False


            match polling_type:
                case "preprocess":
                    logging.info(f"Process {process_name} expects status as {process_status} for polling type {polling_type}")
                    for attempt in range(1, attempts):
                        source_query_df = pd.read_sql(src_table_stmt, conn)
                        logging.info(source_query_df)
                        
                        if source_query_df.empty:
                            logging.info("Polling Record is not present")
                        else:
                            for index, row in source_query_df.iterrows():
                                if process_status.lower() == row['process_status'].strip().lower():
                                    logging.info("Process Status matched")
                                    polling_record_found = True
                                    break
                                else:
                                    logging.info("Process Status did not matched")
                        if polling_record_found:
                            break
                        else:
                            if attempt <= attempts:
                                logging.info("Wait for {} minute(s) after attempt #{}".format(sleep_time_mins, attempt))
                                time.sleep(sleep_time_mins*60)
                    if polling_record_found:
                        src_folder = config['env']['v_srcfilesdir']
                        bq_project_id = config['env']['v_landing_project_id']
                        gcs_bucket = config['env']['v_data_bucket_name']
                        done_file_pattern, formatted_done_file_pattern = qualify_done_file_pattern(polling_config_item)
                        objectfullpath = '{}{}/{}'.format(src_folder, sourcesysnm, formatted_done_file_pattern)
                        gcs_object_path = upload_object_from_file(bq_project_id, gcs_bucket, objectfullpath, formatted_done_file_pattern)
                        logging.info("Done File Created")
                    else:
                        logging.info("Attempts exhausted")
                case "postprocess":
                    source_query_df = pd.read_sql(src_table_stmt, conn)
                    logging.info(source_query_df)
    
                    if source_query_df.empty:
                        src_table_stmt = srcsys_config[src_db_insert_sql]
                        src_table_stmt = self.replacevarwithliterals(src_table_stmt, src_server_name, src_db_name, process_status, process_name, curr_date)
                        logging.info(f"Process {process_name} inserts status as {process_status} for polling type {polling_type}")
                        logging.info(f"SQL Statement {src_table_stmt}")
                        crsr = conn.cursor()
                        crsr.execute(src_table_stmt)
                        conn.commit()
                        logging.info(f"Process {process_name} inserted the status as {process_status} for polling type {polling_type}")
                    else:

                        for index, row in source_query_df.iterrows():
                            curr_process_status = row['process_status']
                            curr_process_date = row['process_date']
                            if curr_process_status.strip() == 'PreUploadCompleted':
                                curr_date = curr_process_date
                        src_table_stmt = srcsys_config[src_db_update_sql]
                        src_table_stmt = self.replacevarwithliterals(src_table_stmt, src_server_name, src_db_name, process_status, process_name, curr_date)
                        logging.info(f"Process {process_name} updates status as {process_status} for polling type {polling_type}")
                        logging.info(f"SQL Statement {src_table_stmt}")
                        crsr = conn.cursor()
                        crsr.execute(src_table_stmt)
                        conn.commit()
                        logging.info(f"Process {process_name} updated the status as {process_status} for polling type {polling_type}")
            
            dt2 = dt.now()
            logging.info("Time Taken " + str(dt2-dt1))         

        except:
            logging.error(traceback.format_exc())
            logging.error("===ERROR: Failure occurred within function===")
            raise SystemExit()

    def process(self, element):
        global bqproject_id
        bqproject_id = config['env']['v_curated_project_id']

        self.readjdbcwritebqtable(bqproject_id)

        logging.info("===END of processing tablelist at {}".format(
            pendulum.now(current_timezone).strftime("%Y-%m-%d %H:%M:%S")))

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
                setuprunnerenv()) | "Read JDBC, Check Polling Record" >> beam.ParDo(jdbctobq())
        else:
            pcoll | "Initialize.." >> beam.Create(
                ["1"]) | "Read JDBC, Check Polling Record" >> beam.ParDo(jdbctobq())

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

def main(sourcesysnm):

    global jobname
    randomstring = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
    jobname = f"{sourcesysnm[:3]}-{polling_type}poll-{process_name.lower()}-{pendulum.now(current_timezone).strftime('%Y%m%d%H%M%S')}-{randomstring}"

    logging.info("===Job Name is {} ===".format(jobname))
    p1 = run()
    dataflow_job_id = p1

    logging.info("===END: Data Pipeline {} at {}===".format(sourcesysnm, pendulum.now(current_timezone).strftime("%Y%m%d-%H:%M:%S")))
    print(dataflow_job_id)

    return dataflow_job_id


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--src_sys_config_file", required=True,
                        help=("Source System Based Config file name"))
    parser.add_argument("--process_name", required=True,
                        help=("Source Process Name"))
    parser.add_argument("--polling_type", required=True,
                        help=("Source Process Polling Type"))
    parser.add_argument("--src_jdbc_lib_path", required=False,
                        help=("JDBC Lib Path"))
    parser.add_argument("--beam_runner", required=True,
                        help=("Beam Runner"))
    
    args = parser.parse_args()

    global srcsys_config_file
    srcsys_config_file = args.src_sys_config_file

    global srcsys_config
    srcsys_config = call_config_yaml(args.src_sys_config_file)

    global process_name
    process_name = args.process_name

    global polling_type
    polling_type = args.polling_type.lower()

    global sourcesysnm
    sourcesysnm = srcsys_config['v_sourcesysnm']

    polling_config_item = next(filter(lambda item: item['process'].lower() == process_name.lower() and item['polling_type'].lower() == polling_type, srcsys_config['schedule']),None)

    logging.info(polling_config_item)

    global src_db_select_sql
    src_db_select_sql = polling_config_item['select_sql']

    global process_status
    global attempts
    global sleep_time_mins
    global done_file
    global src_db_update_sql
    global src_db_insert_sql
    match polling_type:
        case "preprocess":
            process_status = polling_config_item['process_expected_status']
            attempts = polling_config_item['attempts']
            sleep_time_mins = polling_config_item['sleep_time_mins']
            done_file = polling_config_item['done_file']
        case "postprocess":
            process_status = polling_config_item['process_update_status']
            src_db_update_sql = polling_config_item['update_sql']
            src_db_insert_sql = polling_config_item['insert_sql']

    global src_jdbc_lib_path
    src_jdbc_lib_path = args.src_jdbc_lib_path

    logging.info(src_jdbc_lib_path)

    global beam_runner
    beam_runner = args.beam_runner

    logging.info(beam_runner)

    logging.info("===BEGIN: Data Pipeline for src_tbl_list {} at {}===".format(
        sourcesysnm, pendulum.now(current_timezone).strftime("%Y%m%d-%H:%M:%S")))
    main(sourcesysnm)
    