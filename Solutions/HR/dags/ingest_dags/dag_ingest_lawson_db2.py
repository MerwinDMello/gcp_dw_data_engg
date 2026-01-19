from airflow import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime, timedelta
import os
import sys
import time
from airflow.providers.google.cloud.hooks.dataflow import DataflowJobStatus
from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatusSensor
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
import pendulum
import logging


script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, '..', 'utils')
sys.path.append(util_dir)

import common_utilities as cu


config = cu.call_config_yaml("hrg_config.yaml", "hca_hrg_default_vars")
srcsys_config = cu.call_config_yaml("lawson_config.yaml", "hca_hrg_lawson_ingest_dependency")
timezone = pendulum.timezone("US/Central")
done_file =  srcsys_config['v_done_file']

default_args = {
    'owner': 'hca_hrg_atos',
    'depends_on_past': False,
    'email': config['env']['v_failure_email_list'],
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=480)
}
def create_dag(dag_id, schedule, start_date, source_system, frequency,tblist,done_file):
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args, 
        start_date = eval(start_date),
        schedule_interval=schedule,
        catchup=False, 
        max_active_runs=14, 
        template_searchpath='/home/airflow/gcs/dags/sql/',
        tags=[f"{source_system}", f"{frequency}"]
    )
    with dag:    

            # define tasks
        start_job = DummyOperator(task_id='start_dag')
        end_job = DummyOperator(task_id='end_dag')

        now = pendulum.now(timezone).strftime("%Y%m%d")       
        done_file = done_file.replace('YYYYMMDD', now) 
        # bucket=config['env']['v_data_bucket_name'],eim-cs-da-gmek-5764-prod
        file_sensor = GCSObjectsWithPrefixExistenceSensor(
                bucket=config['env']['v_data_bucket_name'],
                prefix=config['env']['v_srcfilesdir']  + source_system + "/" + done_file,
                timeout=18000,
                mode="reschedule",
                task_id="check_lawson_done_file_exists"
            ) 
         # these are full table loads, so truncate before load.
        # with TaskGroup(group_id='truncate_tables') as tg1:
        #     for tgttablename in truncatetablelist:
        #         truncate_table = [BigQueryExecuteQueryOperator(
        #             task_id="truncate_table_" + tgttablename,
        #             dag=dag,
        #             sql="truncate table " +
        #                 config['env']['v_hr_stage_dataset_name'] +
        #                     '.' + tgttablename,
        #             use_legacy_sql=False,
        #             gcp_conn_id=config['env']['v_curated_conn_id']
        #         )]
        with TaskGroup(group_id='run_dataflow_jobs') as tg2:
            for list in tblist:
                with TaskGroup(group_id=f'TG-db2_df_{list}') as tablegroup:
                    run_dataflow_job = [BashOperator(
                        task_id=f"run_plus_db2_{list}",
                        dag=dag,
                    bash_command='sleep $(shuf -i 1-300 -n 1) ; python /home/airflow/gcs/dags/scripts/{} --src_sys_config_file=lawson_config.yaml --src_sys_airflow_varname=hca_hrg_lawson_ingest_dependency --src_tbl_list={}'.format(
                    srcsys_config['v_jdbcbqpytemplate'],list)
                    )]
                    
                    wait_for_python_job_async_done = DataflowJobStatusSensor(
                        task_id="wait_for_python_job_async_done",
                        job_id=f"{{{{ task_instance.xcom_pull(task_ids= 'run_dataflow_jobs.TG-db2_df_{list}.run_plus_db2_{list}')}}}}",
                        expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
                        mode="reschedule",
                        poke_interval=300,
                        location='us-east4',
                        )
                run_dataflow_job >> wait_for_python_job_async_done
            [tablegroup]
        # run dag
                         
        start_job >> file_sensor >> tg2 >> end_job
        return dag
source_system = srcsys_config['v_sourcesysnm']
databasetype = srcsys_config['v_databasetype']
for schedule in srcsys_config['schedule']:
    frequency = schedule["frequency"]
    schedule_interval = schedule["v_schedule_interval"]
    start_date = schedule["start_date"]
    tblist = schedule["tblist"]

    if schedule_interval  == "None":
        interval_range =  "None"
        schedule = None
    else:
        schedule = schedule_interval
        time = schedule.split(" ")
        interval_range = time[1].zfill(2) + "."  + time[0].zfill(2)

    
    dag_id = f'dag_ingest_'+source_system+'_'+ databasetype + '_' + frequency + '_' + interval_range
    globals()[dag_id] = create_dag(dag_id, schedule, start_date, source_system,frequency,tblist,done_file)
    
