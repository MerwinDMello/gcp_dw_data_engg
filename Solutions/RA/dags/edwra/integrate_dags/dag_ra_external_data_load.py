## CREATES DAGS FOR ONDEMAND JOB THAT WRITES DATA BACK TO ORACLE CONCUITY TABLES

from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import  DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.hooks.dataflow import DataflowJobStatus
from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatusSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Connection
from airflow import settings
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from google.cloud import secretmanager
import json 
import yaml
from datetime import datetime, timedelta
import os,sys
import pandas as pd
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain
from airflow.models import DagRun
import pendulum
import logging
from google.cloud import storage


timezone = pendulum.timezone("US/Central")


script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, '..','utils')
sys.path.append(util_dir)
current_timezone = pendulum.timezone("US/Central")
import common_utilities as cu

lob = "edwra"
lob = lob.lower().strip()
lob_abbr = "ra"
config = cu.call_config_yaml(f"{lob_abbr}_config.yaml", f"hca_{lob_abbr}_default_vars")
config_data_load = cu.call_config_yaml("ra_external_data_config.yaml", "ra_external_data_config")
bq_project_id = config['env']['v_curated_project_id']

def access_secret(secret_resourceid: str) -> str:
    '''Retrieve and decode password from GCP Secret Manager based on secret name'''
    client = secretmanager.SecretManagerServiceClient()
    payload = client.access_secret_version(name=secret_resourceid).payload.data.decode("UTF-8")
    return payload


default_args = {
    'owner': f'hca_{lob_abbr}_atos',
    'depends_on_past': False,
    'email': config['env']['v_failure_email_list'],
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=480),
    #params to substitute in sql files
    'params': {
        "param_parallon_ra_stage_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_parallon_ra_stage_dataset_name'],
        "param_parallon_ra_core_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_parallon_ra_core_dataset_name'],
        "param_parallon_ra_base_views_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_parallon_ra_base_views_dataset_name'],
        "param_parallon_ra_views_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_parallon_ra_views_dataset_name'],
        "param_bqutil_fns_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_bqutil_fns_dataset_name'],
        "param_auth_base_views_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_auth_base_views_dataset_name'],
        "param_parallon_ra_audit_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_parallon_ra_audit_dataset_name'],
        "param_parallon_ra_project_name": config['env']['v_curated_project_id'],
        "param_parallon_cur_project_id": config['env']['v_curated_project_id']
        }
    }

def create_dag(dag_id, schedule_interval, start_date, frequency, tbl_name, schema, dependent_dags, oracle_load, tags):
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args, 
        start_date=eval(start_date),
        schedule_interval=schedule_interval, 
        catchup=False, 
        max_active_runs=1, 
        template_searchpath='/home/airflow/gcs/dags/edwra/sql/',
        tags=tags
    )

    with dag:
   
        start_job = DummyOperator(task_id='start_job')
        end_job = DummyOperator(task_id='end_job')

        if tbl_name == "cc_external_data_full_load":
        ## creates dag for cc_external_data_full load source data load from edr_export view

            update_bq_table = BigQueryOperator(
            gcp_conn_id=config['env']['v_curated_conn_id'],
            sql = f'dml/integration/incremental/cc_external_data_day0_load.sql',
            use_legacy_sql=False, 
            retries=0, 
            task_id = f'run_cc_external_data_day0_load.sql'
            )

            with TaskGroup(group_id=f'TG-trigger-dependent-dags') as trigger_grp:
                for dag_id in dependent_dags:

                    trigger_prep_dag = TriggerDagRunOperator(
                    task_id=f"trigger_{dag_id}",
                    trigger_dag_id=f"{dag_id}",
                    )
                trigger_prep_dag
            [trigger_grp]

            start_job >> update_bq_table >> trigger_grp >> end_job
        
        elif oracle_load == "Yes":
        ## creates dags for loading to data to oracle for each table for P1 and P2

            with TaskGroup(group_id=f'TG-concuityLoad-{tbl_name}') as load_grp:
                # runs job to load data to Oracle Concuity
                run_dataflow_job = [BashOperator(
                            task_id=f"run_{tbl_name}",
                            dag=dag,
                            # to start parallel tasks at random times within 5 minutes, sleep n seconds
                            bash_command=f"sleep $(shuf -i 1-300 -n 1) ; python /home/airflow/gcs/dags/edwra/scripts/{config_data_load['v_oracle_to_bq_template']}"
                            f" --src_sys_config_file=ra_external_data_config.yaml --tbl_name={tbl_name} --schema={schema}"
                        )]
                
                wait_for_python_job_async_done = DataflowJobStatusSensor(
                            task_id=f"wait_for_python_job_async_done_{tbl_name}",
                            job_id=f"{{{{ task_instance.xcom_pull(task_ids= 'TG-concuityLoad-{tbl_name}.run_{tbl_name}')}}}}",
                            expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
                            mode="reschedule",
                            poke_interval=300,
                            location=config['env']['v_region'],
                        )
                
                run_dataflow_job >> wait_for_python_job_async_done
                
            [load_grp]

            start_job >> load_grp >> end_job


        else:
            ## creates dags for preprocessing merge steps that run all in BQ


            ## Code to truncate tables in BQ - not used as preprocess is a merge
            # truncate_bq_tables = BigQueryOperator(
            # gcp_conn_id=config['env']['v_curated_conn_id'],
            # sql = f'TRUNCATE TABLE edwra_staging.{tbl_name.lower()}_{schema.lower()};',
            # use_legacy_sql=False, 
            # retries=0, 
            # task_id = 'truncate-bq-tables'
            # )

            with TaskGroup(group_id=f'TG-preprocess-{tbl_name}_{schema}') as prep_grp:

                run_dataflow_job = [BashOperator(
                            task_id=f"run_{tbl_name}_{schema}",
                            dag=dag,
                            # to start parallel tasks at random times within 5 minutes, sleep n seconds
                            bash_command=f"sleep $(shuf -i 1-300 -n 1) ; python /home/airflow/gcs/dags/edwra/scripts/{config_data_load['v_external_load_template']}"
                            f" --src_sys_config_file=ra_external_data_config.yaml --schema={schema} --tbl_name={tbl_name}"
                        )]
                
                wait_for_python_job_async_done = DataflowJobStatusSensor(
                            task_id=f"wait_for_python_job_async_done_{tbl_name}_{schema}",
                            job_id=f"{{{{ task_instance.xcom_pull(task_ids= 'TG-preprocess-{tbl_name}_{schema}.run_{tbl_name}_{schema}')}}}}",
                            expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
                            mode="reschedule",
                            poke_interval=300,
                            location=config['env']['v_region'],
                        )


                run_dataflow_job >> wait_for_python_job_async_done
            [prep_grp]

            with TaskGroup(group_id=f'TG-trigger-dependent-dags') as trigger_grp:
                for dag_id in dependent_dags:
                    dag_id = f"{dag_id}_{schema}_oracle_load".lower()

                    trigger_prep_dag = TriggerDagRunOperator(
                    task_id=f"trigger_{dag_id}",
                    trigger_dag_id=f"{dag_id}",
                    )
                trigger_prep_dag
            [trigger_grp]
        
            start_job  >> prep_grp >> trigger_grp >> end_job

               
    return dag


for schedule in config_data_load['schedule']:
    frequency = schedule["frequency"]
    schedule_interval = schedule["schedule_interval"]
    start_date = schedule["start_date"]
    tbl_name= schedule["tbl_name"]
    schema = schedule["schema"]
    dependent_dags = schedule["dependent_dags"]
    oracle_load = schedule["oracle_load"]

    if schedule_interval == "None":
        interval_range = "None"
        schedule_interval = None
    else:
        time = schedule_interval.split(" ")
        interval_range = time[1].zfill(2) + "."  + time[0].zfill(2)
        interval_range=interval_range.replace("/","-")

    tags = ['ra', frequency]

    # create dags based on tbl_names and schemas, separate dags for each oracle_load
    if schema == "None":
        dag_id = f'dag_ra_{tbl_name}'.lower()
        globals()[dag_id] = create_dag(dag_id, schedule_interval, start_date, frequency, tbl_name, schema, dependent_dags, oracle_load, tags)
    else:
        for schema in schema:
            if oracle_load == 'No':
                dag_id = f'dag_ra_{tbl_name}_{schema}'.lower()
                globals()[dag_id] = create_dag(dag_id, schedule_interval, start_date, frequency, tbl_name, schema, dependent_dags, oracle_load, tags)
            elif oracle_load == 'Yes':
                for tbl in tbl_name:
                    dag_id = f'dag_ra_{tbl}_{schema}_oracle_load'.lower()
                    globals()[dag_id] = create_dag(dag_id, schedule_interval, start_date, frequency, tbl, schema, dependent_dags, oracle_load, tags)