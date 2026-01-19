from paramiko import SSHClient

from paramiko import SSHClient
import paramiko
import airflow, time
from airflow import models
from datetime import datetime, timedelta, date
from airflow.contrib.operators.gcs_to_gcs import GoogleCloudStorageToGoogleCloudStorageOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from utils import ingestion_utilities as iu
from airflow.operators.python import PythonOperator
from google.cloud import storage

current_utc_time = datetime.utcnow()

#Config file

default_args = {
    'owner': 'hca_hrg_atos',
    'depends_on_past': False,
    'start_date': datetime(2023,5,3),
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=240),
    }


with models.DAG('test_sftp', default_args=default_args, catchup=False,schedule_interval=None) as dag:
    
    def sftp_check_download(**context):
        #run date in dag
        run_id = context['run_id']
        is_manual = run_id.startswith('manual__')
        is_scheduled = run_id.startswith('scheduled__')
        if is_manual:
            ts=current_utc_time
            print("manual dag run - ", ts)
        elif is_scheduled:
            ts = context["execution_date"] #add one to execution date 
            ts= ts.add(days=1)
            print("scheduled run date :", ts)
        
        dt = ts.strftime('%m%d%Y')
        str_date = str(dt)
        print("processing date : ", str_date)
        print(paramiko.__version__)
        
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        client.connect('FTSHRSD.inforcloudsuite.com',22,username='HCA_GCP_Prod',password='QLUTBkEQ93zFsWF', banner_timeout=100)
        print("connection successful")
            
        #open sftp
        ftp_client=client.open_sftp()
        print("SFTP opened")
        try:
            listing = ftp_client.listdir()
            print(listing)
            listing = ftp_client.listdir("//Outbound/")
            print(listing)

        except Exception as e:
            print(e)
            
        ftp_client.close()
        print("Connection Closed!")

    sftp_check_download = PythonOperator(
        task_id = 'sftp_check_download',
        dag=dag,
        python_callable = sftp_check_download,
        provide_context = True)
    
    #copy_file_airflow_to_gcs = GoogleCloudStorageToGoogleCloudStorageOperator

    #parse_data

    #load_to_bq
    
sftp_check_download