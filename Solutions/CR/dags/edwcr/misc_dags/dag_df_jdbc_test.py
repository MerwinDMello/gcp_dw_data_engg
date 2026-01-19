from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
import os
import sys
import time
from datetime import datetime, timedelta
import pendulum
import json
import yaml
import logging

timezone = pendulum.timezone("US/Central")

script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, '..', 'utils')
sys.path.append(util_dir)
import common_utilities as cu

lob = "edwcr"
lob = lob.lower().strip()
lob_abbr = lob.replace("edw","")
config = cu.call_config_yaml(f"{lob_abbr}_config.yaml", f"hca_{lob_abbr}_default_vars")
v_source_system = Variable.get("var_source_system")
ingest_config = cu.call_config_yaml(f"{v_source_system}_ingest_config.yaml", f"{v_source_system}_ingest")
jdbc_config = cu.call_config_yaml(f"{v_source_system}_jdbc_test.yaml", "cr_jdbc_test")

v_maxworkers = str(config['env']["v_maxworkers"])
v_numworkers = str(config['env']["v_numworkers"])
v_region = config['env']["v_region"]
v_network = config['env']['v_network']
v_subnetwork = config['env']['v_subnetwork']
v_kmskeyname = config['env']['v_kmskeyname']
v_serviceaccountemail = config['env']['v_serviceaccountemail']
v_gcs_temp_bucket = config['env']['v_gcs_temp_bucket']

v_machine_type = ingest_config['v_machine_type']
v_jdbc_class_name = ingest_config['v_jdbc_class_name']
v_jdbc_jar = config['env']['v_dfjarbucket'] + ingest_config['v_jdbc_jar']

v_jdbc_url = jdbc_config['v_jdbc_url']
v_server_name = jdbc_config['v_server_name']
v_db_name = jdbc_config['v_db_name']

v_cred_secrets_url = config['env']['v_pwd_secrets_url'] + ingest_config['v_cred_secret_name']
creds = json.loads(cu.access_secret(v_cred_secrets_url))
v_user = creds["user"]
v_pwd = creds["pass"]

v_test_sql = jdbc_config['v_test_sql']
v_test_sql = v_test_sql.replace('extracted_server_name', v_server_name)
v_test_sql = v_test_sql.replace('extracted_db_name', v_db_name)
v_test_sql = v_test_sql.replace('v_currtimestamp', str(pendulum.now(timezone))[:23])

v_tgt_bq_tbl = jdbc_config['v_tgt_bq_tbl']
v_tgt_bq_tbl = v_tgt_bq_tbl.replace('v_curated_project_id', config['env']['v_curated_project_id'])

logging.info("Testing for server {}".format(v_server_name))

default_args = {
    'owner': 'hca_cr_atos',
    'depends_on_past': False,
    'start_date': datetime(2023,2,28),
    'email_on_success': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'dataflow_default_options': {
        "maxWorkers": "{}".format(v_maxworkers),
        "numWorkers": "{}".format(v_numworkers),
        "serviceAccountEmail": "{}".format(v_serviceaccountemail),
        "tempLocation": "{}".format(v_gcs_temp_bucket),
        "subnetwork": "{}".format(v_subnetwork),
        "network": "{}".format(v_network),
        "ipConfiguration": "WORKER_IP_PRIVATE",
        "machineType": "{}".format(v_machine_type),
    }
}



def crc32c(data):
    import crcmod
    import six
    crc32c_fun = crcmod.predefined.mkPredefinedCrcFun('crc-32c')
    return crc32c_fun(six.ensure_binary(data))


def encrypt_symmetric(key_name, plaintext):
    from google.cloud import kms
    import base64
    plaintext_bytes = plaintext.encode('utf-8')
    plaintext_crc32c = crc32c(plaintext_bytes)
    kmsclient = kms.KeyManagementServiceClient()
    encrypt_response = kmsclient.encrypt(
        request={'name': key_name, 'plaintext': plaintext_bytes, 'plaintext_crc32c': plaintext_crc32c})

    if not encrypt_response.verified_plaintext_crc32c:
        raise Exception(
            'The request sent to the server was corrupted in-transit.')
    if not encrypt_response.ciphertext_crc32c == crc32c(encrypt_response.ciphertext):
        raise Exception(
            'The response received from the server was corrupted in-transit.')
    cipherstring = str(base64.b64encode(encrypt_response.ciphertext))[2:-1]
    return cipherstring


with DAG(
        dag_id="dag_df_jdbc_test_cr",
        default_args=default_args,
        schedule_interval=None, 
        catchup=False,
        max_active_runs=1,
        tags=[lob_abbr, 'df_test', 'jdbc']

) as dag:
    start_job = DummyOperator(task_id='start_dag')
    end_job = DummyOperator(task_id='end_dag')

    df_test = DataflowTemplatedJobStartOperator(
        task_id="run_df_java_template_" + v_source_system,
        template="gs://dataflow-templates/latest/Jdbc_to_BigQuery",
        job_name="dftestjob-"  + "-" + time.strftime("%Y%m%d%H%M%S"),
        location=v_region,
        parameters={
            "query": "{}".format(v_test_sql),
            "outputTable": "{}".format(v_tgt_bq_tbl),
            "driverClassName": "{}".format(v_jdbc_class_name),
            "driverJars": "{}".format(v_jdbc_jar),
            "bigQueryLoadingTemporaryDirectory": "{}".format(v_gcs_temp_bucket),
            "KMSEncryptionKey": "{}".format(v_kmskeyname),
            "connectionURL": "{}".format(encrypt_symmetric(v_kmskeyname, v_jdbc_url)),
            "username": "{}".format(encrypt_symmetric(v_kmskeyname, v_user)),
            "password": "{}".format(encrypt_symmetric(v_kmskeyname, v_pwd))
        }
    )

    start_job >> df_test >> end_job