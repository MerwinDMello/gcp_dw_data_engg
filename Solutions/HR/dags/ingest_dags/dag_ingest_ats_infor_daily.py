from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime, timedelta
import os
import sys
import time
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.hooks.dataflow import DataflowJobStatus
from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatusSensor
import pendulum


script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, '..', 'utils')
sys.path.append(util_dir)
import common_utilities as cu


config = cu.call_config_yaml("hrg_config.yaml", "hca_hrg_default_vars")
srcsys_config = cu.call_config_yaml("ats_config.yaml", "ats_vars")
timezone = pendulum.timezone("US/Central")

default_args = {
    'owner': 'hca_hrg_atos',
    'depends_on_past': False,
    'email': config['env']['v_failure_email_list'],
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=240)
}

with DAG(
    dag_id="dag_ingest_ats_infor_daily_tbllist",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval=srcsys_config['v_schedule_interval'],
    catchup=False,
    max_active_runs=1,
    tags=["ats_infor", "daily"]
) as dag:

    # define tasks
    start_job = DummyOperator(task_id='start_dag')
    end_job = DummyOperator(task_id='end_dag')

    with TaskGroup(group_id='run_dataflow_jobs') as tg1:
        for i in range(1, 7):
            with TaskGroup(group_id=f'TG-ats_infor_df_tblist-{i}') as tablegroup:
                run_dataflow_job = [BashOperator(
                    task_id="run_ats_infor_df_srctablelist{}".format(i),
                    dag=dag,
                    bash_command='sleep $(shuf -i 1-30 -n 1) ; python /home/airflow/gcs/dags/scripts/{} --src_sys_config_file=ats_config.yaml --src_sys_airflow_varname=ats_vars --src_tbl_list=srctablelist{}'.format(
                        srcsys_config['v_jdbcbqpytemplate'], i, i)
                )]
                wait_for_python_job_async_done = DataflowJobStatusSensor(
                        task_id="wait_for_python_job_async_done",
                        job_id=f"{{{{ task_instance.xcom_pull(task_ids= 'run_dataflow_jobs.TG-ats_infor_df_tblist-{i}.run_ats_infor_df_srctablelist{i}')}}}}",
                        expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
                        mode="reschedule",
                        poke_interval=300,
                        location='us-east4',
                        )
            run_dataflow_job >> wait_for_python_job_async_done
        [tablegroup]
    # run dag

    start_job >> tg1 >> end_job
