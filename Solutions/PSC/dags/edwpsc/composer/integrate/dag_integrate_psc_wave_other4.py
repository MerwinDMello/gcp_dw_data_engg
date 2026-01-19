from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator
import json
import yaml
from datetime import timedelta
import os, sys
from airflow.utils.task_group import TaskGroup
import pendulum
import logging
from google.cloud import storage

script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, '../..','utils')
sys.path.append(util_dir)
current_timezone = pendulum.timezone("US/Central")
import common_utilities as cu

lob = "edwpsc"
lob = lob.lower().strip()
lob_abbr = lob.replace("edw","")
src_system = "psc_wave_other4"
src_system = src_system.lower().strip()
sql_src = src_system.replace("psc_","")
config = cu.call_config_yaml(f"{lob_abbr}_config.yaml", f"hca_{lob_abbr}_default_vars")
lob = config['env']['v_lob']
bq_project_id = config['env']['v_curated_project_id']

default_args = {
    'owner': f'hca_{lob_abbr}_atos',
    'depends_on_past': False,
    'email': False,
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=480),
    'params': {
        f"param_{lob_abbr}_stage_dataset_name": f"{bq_project_id}.{config['env'][f'v_{lob_abbr}_stage_dataset_name']}",
        f"param_{lob_abbr}_core_dataset_name": f"{bq_project_id}.{config['env'][f'v_{lob_abbr}_core_dataset_name']}",
        f"param_{lob_abbr}_base_views_dataset_name": f"{bq_project_id}.{config['env'][f'v_{lob_abbr}_base_views_dataset_name']}",
        f"param_{lob_abbr}_views_dataset_name": f"{bq_project_id}.{config['env'][f'v_{lob_abbr}_views_dataset_name']}",
        f"param_{lob_abbr}_audit_dataset_name": f"{bq_project_id}.{config['env'][f'v_{lob_abbr}_audit_dataset_name']}",
        f"param_auth_base_views_dataset_name": f"{bq_project_id}.{config['env'][f'v_auth_base_views_dataset_name']}",
    }
}
def create_dag(dag_id, schedule, start_date, dependency, source_system, frequency, tags, done_file, trigger_dag_ids):
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        start_date=eval(start_date),
        schedule_interval=schedule,
        is_paused_upon_creation=True,
        catchup=False,
        max_active_runs=1,
        concurrency=30,
        template_searchpath=f'/home/airflow/gcs/dags/{lob}/load_sql/',
        tags=tags
    )

    with dag:
        start_job = DummyOperator(task_id='start_job')
        end_job = DummyOperator(task_id='end_job')

        # ==============================
        # done_file handling
        # ==============================
        if done_file != "None":
            try:
                if frequency == "daily":
                    now = pendulum.now(current_timezone).strftime("%Y%m%d")
                    prev = (pendulum.now(current_timezone).subtract(days=7)).strftime("%Y%m%d")
                elif frequency == "weekly":
                    now = pendulum.yesterday(current_timezone).strftime("%Y%m%d")
                    prev = (pendulum.yesterday(current_timezone).subtract(days=7)).strftime("%Y%m%d")
                elif frequency == "monthly":
                    now = pendulum.now(current_timezone).strftime("%Y%m%d")
                    prev = (pendulum.now(current_timezone).subtract(days=30)).strftime("%Y%m%d")
            except:
                logging.info(f"================={frequency} is not valid =================")

            done_file_to_delete = done_file.replace('YYYYMMDD', prev)
            done_file = done_file.replace('YYYYMMDD', now)

            file_sensor = GCSObjectsWithPrefixExistenceSensor(
                bucket=gcs_bucket,
                prefix=src_folder + source_system + "/" + done_file,
                timeout=10800,
                mode="reschedule",
                task_id="check_done_file_exists"
            )

            delete_old_done_files = PythonOperator(
                task_id='delete_old_done_files',
                python_callable=cu.delete_files_with_filename,
                op_kwargs={
                    'gcs_bucket_name': gcs_bucket,
                    'gcs_folder': src_folder + source_system,
                    'gcs_project_id': gcs_project_id,
                    'target_filename': done_file
                }
            )

        table_groups = [] 


        for item in dependency:
            table = item["table"]
            has_sensor = item["has_sensor"]
            validation_file_list = item["validation"]
            file_list = [f"{file_name['name']}" for file_name in validation_file_list]

            with TaskGroup(group_id=f'TG-table-{table}') as table_grp:
                sql_arr = []

                for idx, sql in enumerate(item["sql"]):
                    update_bq_table = BigQueryInsertJobOperator(
                        task_id=f'run_{sql["name"]}',
                        gcp_conn_id=config['env']['v_curated_conn_id'],
                        pool='bigquery_pool',
                        retries=0,
                        configuration={
                            "query": {
                                "query": f"{{% include 'integrate/{sql_src}/{sql['name']}' %}}",
                                "useLegacySql": False,
                            }
                        },
                    )
                    if idx > 0:
                        sql_arr[-1] >> update_bq_table
                    sql_arr.append(update_bq_table)

                last_sql_task = sql_arr[-1]
                

                update_audit_table = PythonOperator(
                    task_id=f'audit_table_{table}',
                    python_callable=cu.executevalidationsqls,
                    provide_context=True,
                    op_kwargs={
                        'bq_table': table,
                        'task_id': last_sql_task.task_id, 
                        'dag_id': dag_id,
                        'source': source_system,
                        'config': config,
                        'replacements': default_args['params'],
                        'file_list': file_list
                    }
                )

                sql_arr[-1] >> update_audit_table


            if has_sensor == 'Yes':
                with TaskGroup(group_id=f'TG-sensor-{source_system}-{table}') as sensor_grp:
                    for sensor in item["sensor"]:
                        ext_dag_id = sensor["dag_id"]
                        ext_task_id = sensor["task_id"]
                        schedule = sensor["schedule"]
                        cycle_age = sensor.get("cycle_age", "current")
                        sensor_dag_frequency = sensor.get("sensor_dag_frequency", "daily")
                        hour_interval = int(sensor.get("hour_interval", 0))
                        day_interval = sensor.get("day_interval", 0)

                        check_task_completion = ExternalTaskSensor(
                            task_id=f"Check_status_{ext_dag_id}",
                            external_dag_id=ext_dag_id,
                            external_task_id=ext_task_id,
                            timeout=10800,
                            execution_date_fn=cu.get_execution_date,
                            params={
                                "schedule": schedule,
                                "frequency": frequency,
                                "cycle_age": cycle_age,
                                "sensor_dag_frequency": sensor_dag_frequency,
                                "hour_interval": hour_interval,
                                "day_interval": day_interval
                            },
                            allowed_states=["success"],
                            failed_states=["failed", "skipped"],
                            mode="reschedule"
                        )
                        [check_task_completion]
                table_groups.append(sensor_grp)
            else:
                table_groups.append(table_grp)

        # chaining start → all tables → end
        if done_file == "None":
            start_job >> table_groups >> end_job
        else:
            start_job >> file_sensor >> table_groups >> delete_old_done_files >> end_job


        if trigger_dag_ids:
            with TaskGroup(group_id=f'TG-trigger_dag') as trigger_dag_grp:
                for trigger_dag_id in trigger_dag_ids:
                    trigger_dag = TriggerDagRunOperator(
                        task_id=f"trigger_dag_{trigger_dag_id}",
                        trigger_dag_id=trigger_dag_id,
                    )
            table_groups >> trigger_dag_grp >> end_job

    return dag

integrate_config = cu.call_config_yaml(f"{src_system}_integrate_config.yaml", f"{src_system}_integrate")

for integrate in integrate_config['integrate']:
    frequency = integrate["frequency"]
    schedule = integrate["schedule"]
    start_date = integrate["start_date"]
    source_system = integrate["source_system"]
    db_type = integrate["type"]
    done_file = integrate["done_file"]
    dag_name_suffix = integrate["dag_name_suffix"]

    if schedule == "None":
        schedule = None

    # loop over each server (server04, server06, etc.)
    for server_name, server_block in integrate.items():
        if server_name in ["source_system", "frequency", "dag_name_suffix", "start_date",
                           "schedule", "type", "done_file"]:
            continue

        dependency = server_block.get("tables", server_block)   
        trigger_dag_ids = server_block.get("trigger_dag_ids", [])

        tags = [source_system, dag_name_suffix, src_system, server_name]

        if frequency == "None":
            dag_id = f"dag_integrate_{source_system}_adhoc_{dag_name_suffix}-{server_name}_{db_type}"
            tags.append("adhoc")
        else:
            dag_id = f"dag_integrate_{source_system}_{dag_name_suffix}-{server_name}_{db_type}_{frequency}"
            tags.append(frequency)

        globals()[dag_id] = create_dag(
            dag_id, schedule, start_date, dependency,
            source_system, frequency, tags, done_file, trigger_dag_ids
        )


