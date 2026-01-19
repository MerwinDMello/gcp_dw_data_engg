import airflow
import time
from airflow import models
from datetime import datetime, timedelta, date
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from utils import ingestion_utilities as iu
from airflow.operators.python import PythonOperator
from google.cloud import storage
from google.cloud import secretmanager

current_utc_time = datetime.utcnow()

# Config file

default_args = {
    'owner': 'hca_hrg_atos',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 18),
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=240),
}

with models.DAG('test_pgp_enwisen', default_args=default_args, catchup=False, schedule_interval=None) as dag:

    def access_secret(secret_resourceid):
        #logging.info("Get Secret Version {}".format(secret_resourceid))
        client = secretmanager.SecretManagerServiceClient()
        payload = client.access_secret_version(
            secret_resourceid).payload.data.decode("UTF-8")
        return payload

    def test_pgp(**context):
        # run date in dag
        private_key = access_secret("projects/318104889357/secrets/enwisen_pgp_private_key/versions/latest")
        print(private_key)

        public_key = access_secret("projects/318104889357/secrets/enwisen_pgp_public_key/versions/latest")
        print(public_key)

    test_pgp_decrypt = PythonOperator(
        task_id='test_pgp_enwisen',
        dag=dag,
        python_callable=test_pgp,
        provide_context=True)

test_pgp_decrypt