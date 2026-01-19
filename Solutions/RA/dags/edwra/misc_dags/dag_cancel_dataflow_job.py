import os
import sys
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStopJobOperator

script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, '..', 'utils')
sys.path.append(util_dir)
# import common_utilities after setting the path for utils
import common_utilities as cu
from google.cloud import storage

config = cu.call_config_yaml("ra_config.yaml", "hca_ra_default_vars")
gcp_region = config["env"]["v_region"]


default_args = {
    'owner': 'hca_ra_atos',
    'start_date': datetime(2024, 6, 1),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=240),
}

with DAG(
    dag_id="dag_cancel_dataflow_job",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    template_searchpath='/home/airflow/gcs/dags/edwra/sql/',
    tags=["ra", "adhoc"]
) as dag:
    df_job_prefix = Variable.get("var_ra_cancel_df_job_prefix")

    start_job = DummyOperator(task_id='start_job')
    end_job = DummyOperator(task_id='end_job')
    stop_dataflow_job = DataflowStopJobOperator(
        task_id="stop_dataflow_job",
        location=gcp_region,
        job_name_prefix=df_job_prefix #"ra-p-ra-oracle-ingest-dependency-daily-tblist0-p1"
    )

    start_job >> stop_dataflow_job >> end_job
