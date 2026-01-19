import logging
import os
import decimal
import traceback
from google.cloud import bigquery
import apache_beam as beam
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
import math
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


class bqtoconcuity(beam.DoFn):

    def readbqwriteoracle(self, bqproject_id):
        import jaydebeapi
        import oracledb
        import pendulum
        import io
        import re
        from jaydebeapi import _DEFAULT_CONVERTERS
        from datetime import datetime
        import decimal
        from sqlalchemy import create_engine
        decimal_context = decimal.Context(prec=13)

        bq_tbl_name = f"{tbl_name}_{schema}".lower()

        bqclient = bigquery.Client(bqproject_id)
        dataset_ref = bqclient.dataset(bq_stage_dataset)
        table_ref = dataset_ref.table(bq_tbl_name)
        table = bqclient.get_table(table_ref)
        f = io.StringIO("")
        bqclient.schema_to_json(table.schema, f)
        tblschema = json.loads(f.getvalue())
        # logging.info(tblschema)

        # Override jaydebeapi package default converter to fix milliseconds processing bug
        def _to_datetime(rs, col):

            java_val = rs.getTimestamp(col)
            if not java_val:
                return
            d = datetime.strptime(str(java_val)[:19],"%Y-%m-%d %H:%M:%S")
            d = d.replace(microsecond=java_val.getNanos() // 1000)
            return str(d)

        _DEFAULT_CONVERTERS.update({"TIMESTAMP" : _to_datetime})


        logging.info("===Starting process to extract edwra_staging.cc_external_data_full_load and load {} at {}===".format(
         bq_tbl_name, time.strftime("%Y%m%d-%H:%M:%S")))

        ## set primary keys for the merge and create conversion dict for column names to target column names
        if tbl_name == 'ACCT_PYR_EXT_DATA_STAGE':
            primary_keys = ('acct_pyr_id', 'org_id')
            conv_dict  = {
               "src_sys_id" : "src_sys_id",
               "misc_num_2" : "iplan_id",
               "misc_num_3" : "iplan_order_num",
               "acct_pyr_id" : "account_payor_id",
               "org_id" : "org_id",
               "misc_dt_1" : "first_doc_request_mr_date",
               "misc_dt_2" : "last_doc_request_mr_date",
               "misc_dt_3" : "first_doc_sent_mr_date",
               "misc_dt_4" : "last_doc_sent_mr_date",
               "misc_dt_5" : "first_doc_request_ib_date",
               "misc_dt_6" : "last_doc_request_ib_date",
               "misc_dt_7" : "first_doc_sent_ib_date",
               "misc_dt_8" : "last_doc_sent_ib_date",
               "misc_dt_9" : "first_doc_request_date",
               "misc_dt_10" : "last_doc_request_date",
               "misc_dt_11" : "first_doc_sent_date",
               "misc_dt_12" : "last_doc_sent_date",
               "misc_dt_13" : "first_doc_received_date",
               "misc_dt_14" : "last_doc_received_date",
               "misc_dt_15" : "first_doc_approved_date",
               "misc_dt_16" : "last_doc_approved_date",
               "misc_dt_17" : "first_doc_denied_date",
               "misc_dt_18" : "last_doc_denied_date",
               "misc_char_2" : "payer_type_code",
               "misc_char_1" : "sub_payor_group_id",
               "misc_char_3" : "treatment_authorization_num",
               "misc_char_4" : "credit_status",
               "misc_dt_19" : "refund_create_date",
               "misc_char_5" : "refund_requested_by",
               "misc_char_6" : "split_bill_ind",
               "misc_dt_20" : "last_scrted_appl_date_time",
               "misc_char_7" : "scripted_overpayment_desc",
               "misc_dt_21" : "last_letter_sent_date_time",
               "misc_char_8" : "takeback_follow_up_ltr_ind",
               "misc_dt_22" : "pyr_ref_last_update_date_time",
               "misc_dt_23" : "recoup_date",
               "misc_char_9" : "tmr_ma_ereq_auto_nonbil_ind",
               "misc_char_10" : "artiva_claim_num",
               "misc_char_11" : "recoup_flag",
               #"misc_char_12" : "ground_transportation_ind",              
               "creation_dt" : "current_date()",
               "creation_user" : "'rpt_extract'"

            }

        elif tbl_name == 'ACCT_EXT_DATA_STAGE':
            primary_keys = ('acct_id', 'org_id')
            conv_dict  = {
               "src_sys_id" : "src_sys_id",
               "acct_id" : "account_id",
               "org_id" : "org_id",
               "misc_char_1" : "discrepancy_source_desc",
               "misc_char_2" : "reimbursement_impact_desc",
               "misc_dt_1" : "discrepancy_date_time",
               "misc_dt_2" : "request_date_time",
               "misc_char_3" : "reprocess_reason_text",
               "misc_char_4" : "status_desc",
               "misc_char_5" : "esl_level_1_desc",
               "misc_char_6" : "esl_level_2_desc",
               "misc_char_7" : "esl_level_3_desc",
               "misc_char_8" : "esl_level_4_desc",
               "misc_char_9" : "esl_level_5_desc",
               "misc_char_10" : "chois_product_line_code",
               "misc_char_11" : "chois_product_line_desc",
               "misc_char_12" : "denial_in_midas_status",
               "misc_dt_3" : "midas_date_of_denial",
               "misc_char_13" : "all_days_approved_ind",
               "misc_char_14" : "ptp_performed",
               "misc_char_15" : "cm_xf_ind",
               "misc_char_16" : "cm_xg_ind",
               "misc_dt_4" : "cm_last_xf_code_applied_date",
               "misc_dt_5" : "cm_last_xg_code_applied_date",
               "misc_char_17" : "midas_acct_num",
               "misc_char_18" : "last_appeal_status",
               "misc_char_19" : "last_appeal_employee_id",
               "misc_char_20" : "last_appeal_employee_name",
               "misc_char_21" : "last_conc_review_disp",
               "misc_char_22" : "midas_principal_payer_auth_num",
               "misc_char_23" : "midas_principal_pyr_auth_type",
               "misc_char_24" : "cm_last_iq_revi_crit_met_desc",
               "misc_char_25" : "cm_last_iq_review_version_desc",
               "misc_char_26" : "cm_last_iq_review_subset_desc",
               "misc_char_27" : "status_cause_name",
               "misc_dt_6" : "last_appeal_date",
               "misc_char_28" : "drg_medical_surgical_ind",
               "misc_char_29" : "apr_drg_code",
               "misc_char_30" : "apr_drg_grouper_name",
               "misc_char_31" : "apr_severity_of_illness_desc",
               "misc_char_32" : "apr_risk_of_mortality_desc",
               "misc_char_33" : "cond_code_xf_xg_ind",
               "misc_char_38" : "cond_code_nu_ind",
               "misc_char_36" : "cond_code_ne_ind",
               "misc_char_34" : "cond_code_ns_ind",
               "misc_char_35" : "cond_code_np_ind",
               "misc_char_37" : "cond_code_no_ind",
               "misc_char_39" : "pdu_determination_reason_desc",
               "misc_num_1" : "refund_amt",
               "misc_dt_7" : "refund_create_date",
               "misc_char_40" : "refund_requested_by",
               "misc_char_41" : "covid_positive_flag",
               "misc_num_2" : "ins1_payor_balance_amt",
               "misc_num_3" : "ins2_payor_balance_amt",
               "misc_num_4" : "ins3_payor_balance_amt",
               "misc_dt_8" : "first_ptp_completed_date",
               "misc_char_42" : "first_strength_of_case",
               "misc_dt_9" : "last_ptp_completed_date",
               "misc_char_43" : "last_strength_of_case",
               "misc_num_5" : "total_ptp_closure_stts_compltd",
               "misc_num_6" : "total_midnights",
               "misc_num_7" : "total_inhouse_midnights",
               "misc_char_44" : "gov_sec_tert_iplan",
               "misc_dt_10" : "pat_ref_last_update_date_time",
               "misc_dt_11" : "presentation_date",
               "misc_char_45" : "dna_clinical_rationale_1",
               "misc_char_46" : "dna_clinical_rationale_2",
               "misc_char_47" : "dna_clinical_rationale_3",
               "misc_char_48" : "dna_clinical_rationale_4",
               "misc_char_49" : "dna_clinical_rationale_5",
               "misc_char_50" : "dna_clinical_rationale_6",
               "misc_char_51" : "dna_clinical_rationale_6_notes",
               "misc_char_52" : "dna_ip_only_proc_waterpark_ind",
               "misc_char_53" : "dna_ip_only_proc_user_ind",
               "misc_char_54" : "psr_agree_disagree_flag",
               "misc_char_55" : "ground_transportation_ind",
               "misc_dt_12" : "first_psr_note_date_time",
               "misc_dt_13" : "last_psr_note_date_time",
               #"misc_dt_14" : "recoup_date",               
                "creation_dt" : "current_date()",
               "creation_user" : "'rpt_extract'"
            }
            
        logging.info('Build merge statement')
        src_query = ra_external_data[f'{tbl_name}_sql']
        src_query = src_query.format(schema=schema)
        update_fields = [col['name'] for col in tblschema if col['name'] not in primary_keys]
        insert_part = ", ".join(col['name'] for col in tblschema)
        valuespart = ", ".join(conv_dict[col['name']] for col in tblschema)

        merge_query = f'MERGE INTO edwra_staging.{bq_tbl_name} T \nUSING ({src_query}) S ON\n'

        for index, primary_key in enumerate(primary_keys):
            if index == 0:
                merge_query +="T.{} = S.{}\n".format(primary_key, conv_dict[primary_key])
            else:
                merge_query +=" and T.{} = S.{}".format(primary_key, conv_dict[primary_key])
        merge_query += "\nWHEN MATCHED THEN UPDATE SET\n"
        for index, col in enumerate(update_fields):
            if index == 0 and col not in ('creation_user','creation_dt'):
                merge_query += "T.{} = S.{}".format(col, conv_dict[col])
            elif col in ('creation_user','creation_dt'):
                merge_query += ",\nT.{} = {}".format(col, conv_dict[col])
            else:
                merge_query += ",\nT.{} = S.{}".format(col, conv_dict[col])



        merge_query+=f' when not matched then insert ({insert_part})\n'
        merge_query+=f' VALUES ({valuespart})'

        logging.info(merge_query)

        logging.info('Running merge query')
        bqclient.query(merge_query).result()
        logging.info(f'Rows successfully merged into {bq_tbl_name}')


    def process(self, element):

        try:
            global bqproject_id
            bqproject_id = config['env']['v_curated_project_id']
        
            self.readbqwriteoracle(bqproject_id)

        except:
            logging.error(
               "===ERROR: Failure occurred within Process function===")
            logging.error(traceback.format_exc())
            raise SystemExit()


def run():
    pipeline_args = [
       "--project", config['env']['v_proc_project_id'],
       "--service_account_email", config['env']['v_serviceaccountemail'],
       "--job_name", jobname.replace("_","-"),
       "--runner", config['env']['v_runner'],
       "--network", config['env']["v_network"],
       "--subnetwork", config['env']["v_subnetwork"],
       "--staging_location", config['env']["v_dfstagebucket"],
       "--temp_location", config['env']["v_gcs_temp_bucket"],
       "--region", config['env']["v_region"],
       "--save_main_session",
       "--num_workers", str(config['env']["v_numworkers"]),
       "--max_num_workers","1",
       "--no_use_public_ips",
       "--dataflow_service_options, enable_prime",
       "--autoscaling_algorithm","THROUGHPUT_BASED",
       "--worker_machine_type",  ra_external_data['v_machine_type'],
       "--setup_file", '{}setup.py'.format(utilpath)
    ]


    try:
        logging.info(
           "===Apache Beam Run with pipeline options started===")
        pcoll = beam.Pipeline(argv=pipeline_args)
        if config['env']['v_runner'] == 'DataflowRunner':
            pcoll |"Initialize" >> beam.Create(["1"]) | 'Setup Dataflow Worker' >> beam.ParDo(
                setuprunnerenv()) |"Read from BQ, write to Oracle" >> beam.ParDo(bqtoconcuity())
            logging.info("===Submitting Asynchronous Dataflow Job===")
            p = pcoll.run()
            dataflow_job_id = p.job_id()
        else:
            pcoll |"Initialize.." >> beam.Create(
                ["1"]) |"Read JDBC, Write to Google BigQuery" >> beam.ParDo(bqtoconcuity())
            logging.info("===Submitting Asynchronous Dataflow Job===")
            p = pcoll.run()
            dataflow_job_id = '123' #p.job_id()

        logging.info("===Submitted Asynchronous Dataflow Job, Job id is" + dataflow_job_id +" ====")
        return dataflow_job_id

    except:
        logging.exception("===ERROR: Apache Beam Run Failed===")
        raise SystemExit()


def main(tbl_name):
    global jobname
    randomstring = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))

    if tbl_name == 'cc_external_data_full_load':
        jobname ="ra-concuity-truncate-oracle-tables-" + time.strftime("%Y%m%d%H%M%S")
    else:
        jobname ="ra-concuity-" + tbl_name.lower() + '-' + schema.lower() + '-' + time.strftime("%Y%m%d%H%M%S") #+ '-' + randomstring

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
    parser.add_argument("--schema", required=True,
                        help=("Schema to identify P1 or P2 from bq"))
    parser.add_argument("--tbl_name", required=True,
                        help=("Concuity table name"))
    
    args = parser.parse_args()

    global srcsys_config_file
    srcsys_config_file = args.src_sys_config_file

    global ra_external_data
    ra_external_data = call_config_yaml(args.src_sys_config_file,"ra_external_data_config")

    global schema
    schema = args.schema

    global tbl_name
    tbl_name = args.tbl_name

    global bq_stage_dataset
    bq_stage_dataset = config['env']['v_parallon_ra_stage_dataset_name']

    logging.info("===BEGIN: Data Pipeline for load to Concuity table {} at {}===".format(
        tbl_name, time.strftime("%Y%m%d-%H:%M:%S")))
    main(tbl_name)