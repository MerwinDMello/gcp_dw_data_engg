from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import  DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import os
import sys
import logging
import time
import pendulum
import pandas as pd
from math import ceil



script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, '..', 'utils')
sys.path.append(util_dir)

import common_utilities as cu
config = cu.call_config_yaml("hrg_config.yaml", "hca_hrg_default_vars")
srcsys_config = cu.call_config_yaml("rdm_config.yaml", "rdm_vars")
timezone = pendulum.timezone("US/Central")
sourcesysnm = srcsys_config['v_sourcesysnm']
type = srcsys_config['v_databasetype']
frequency = srcsys_config['v_frequency']
start_date = srcsys_config['v_start_date']
sch_time = srcsys_config["v_schedule_interval"]
if sch_time == "None":
    interval_range =  "None"
    sch_time = None
else:
    sch_time = srcsys_config["v_schedule_interval"].split(" ")
    interval_range = sch_time[1].zfill(2) + "."  + sch_time[0].zfill(2)


default_args = {
    'owner': 'hca_hrg_atos',
    'depends_on_past': False,
    'email': config['env']['v_failure_email_list'],
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'dataflow_default_options': {
        "maxWorkers": "{}".format(config['env']['v_maxworkers']),
        "numWorkers": "{}".format(config['env']['v_numworkers']),
        "serviceAccountEmail": "{}".format(config['env']['v_serviceaccountemail']),
        "tempLocation": "{}".format(config['env']['v_gcs_temp_bucket']),
        "subnetwork": "{}".format(config['env']['v_subnetwork']),
        "network": "{}".format(config['env']['v_network']),
        "ipConfiguration": "WORKER_IP_PRIVATE"
    }
}

with DAG(
    dag_id=f'dag_ingest_'+sourcesysnm+'_'+type+'_' + frequency + '_' + interval_range,
    default_args=default_args,
    start_date=eval(start_date),
    schedule_interval=srcsys_config['v_schedule_interval'],
    catchup=False,
    max_active_runs=1,
    tags=[f"{sourcesysnm}", f"{frequency}"]
) as dag:

    start_job = DummyOperator(task_id='start_dag')
    end_job = DummyOperator(task_id='end_dag')

    with TaskGroup(group_id='run_dataflow_jobs') as tg1:
        for i in range(1, 2):
            run_dataflow_job = [BashOperator(
                task_id="run_ats_infor_df_srctablelist{}".format(i),
                dag=dag,
                bash_command='sleep $(shuf -i 1-30 -n 1) ; python /home/airflow/gcs/dags/scripts/{} --src_sys_config_file=rdm_config.yaml --src_sys_airflow_varname=rdm_vars --src_tbl_list=srctablelist{}'.format(
                    srcsys_config['v_jdbcbqpytemplate'], i, i)
            )]
        start_job >> run_dataflow_job >> end_job
   

    