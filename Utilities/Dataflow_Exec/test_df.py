import logging
import os
from datetime import datetime
from datetime import timedelta
import apache_beam as beam
import sys
import yaml
import json
import argparse
from apache_beam.io.gcp.internal.clients import bigquery

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

config = call_config_yaml("edwpbs/pbs_config.yaml")

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
        "--worker_machine_type", config_df['v_machine_type'],
        "--setup_file", '{}setup.py'.format(utilpath)

    ]

    try:
        logging.info(
            "===Apache Beam Run with pipeline options started===")
        pcoll = beam.Pipeline(argv=pipeline_args)

        table_schema = bigquery.TableSchema()

    # Fields that use standard types.

        full_name_schema = bigquery.TableFieldSchema()
        full_name_schema.name = 'fullName'
        full_name_schema.type = 'string'
        full_name_schema.mode = 'required'
        table_schema.fields.append(full_name_schema)

        age_schema = bigquery.TableFieldSchema()
        age_schema.name = 'age'
        age_schema.type = 'integer'
        age_schema.mode = 'nullable'
        table_schema.fields.append(age_schema)

        gender_schema = bigquery.TableFieldSchema()
        gender_schema.name = 'gender'
        gender_schema.type = 'string'
        gender_schema.mode = 'nullable'
        table_schema.fields.append(gender_schema)

        pcoll | beam.Create([
            {
                'fullName': 'M1', 'age': 24, 'gender': 'Male'
            },
            {
                'fullName': 'F2', 'age': 24, 'gender': 'Female'
            }
        ]) | 'write' >> beam.io.WriteToBigQuery(
            'hca-hin-dev-cur-parallon:edwpbs.test1',
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)

        p = pcoll.run()
        logging.info("===Apache Beam Run completed successfully===")
        dataflow_job_id = p.job_id()
        logging.info( "===Submitted Asynchronous Dataflow Job, Job id is " + dataflow_job_id + " ====")
        return dataflow_job_id

    except:
        logging.exception("===ERROR: Apache Beam Run Failed===")
        raise SystemExit()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--config_file", required=True, help=("Config file name"))

    args = parser.parse_args()

    config_file = args.config_file
    global config_df
    config_df = call_config_yaml(config_file)

    dt1 = datetime.now()
    global jobname
    jobname = "test" + (dt1).strftime("%Y%m%d%H%M%S")

    logging.info("===Job Name is {} ===".format(jobname))

    run()