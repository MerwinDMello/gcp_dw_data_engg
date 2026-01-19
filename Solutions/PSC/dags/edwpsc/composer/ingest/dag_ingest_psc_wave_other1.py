from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.providers.google.cloud.hooks.dataflow import DataflowJobStatus
from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatusSensor
from google.api_core import exceptions

from google.cloud import bigquery
from google.cloud import secretmanager

from datetime import datetime, timedelta
import os, sys, re, json, logging
import time
import pendulum
import random

from jinja2 import Template

# =============================== Utilities & Config ===============================
script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, '../..', 'utils')
ingest_dir = os.path.join(script_dir, '../..', 'load_sql', 'ingest')
sys.path.append(util_dir)
import common_utilities as cu

lob = "edwpsc"
lob = lob.lower().strip()
lob_abbr = lob.replace("edw", "")

# 1) env/defaults YAML (contains v_dfjarbucket, datasets, buckets, region, servers, etc.)
config = cu.call_config_yaml(f"{lob_abbr}_config.yaml", f"hca_{lob_abbr}_default_vars")

# 2) Ingest YAML (contains schedule, tblist)
src_system_file_prefix = "psc_wave_other1"
srcsys_config = cu.call_config_yaml(f"{src_system_file_prefix}_ingest_config.yaml", f"{src_system_file_prefix}_ingest")

current_timezone = pendulum.timezone("US/Central")

gcp_conn_id   = config['env']['v_curated_conn_id']
bq_project_id = config['env']['v_curated_project_id']
staging_dataset = config['env'][f'v_{lob_abbr}_stage_dataset_name']
core_dataset = config['env'][f'v_{lob_abbr}_core_dataset_name']
audit_dataset = config['env'][f'v_{lob_abbr}_audit_dataset_name']
control_table_name = config['env']['v_controltablename']
audit_table_name = config['env']['v_audittablename']

params = {
        f"param_{lob_abbr}_stage_dataset_name": f"{bq_project_id}.{staging_dataset}",
        f"param_{lob_abbr}_core_dataset_name": f"{bq_project_id}.{core_dataset}",
        f"param_{lob_abbr}_audit_dataset_name": f"{bq_project_id}.{audit_dataset}",
        "param_control_table_name": control_table_name,
        "param_audit_table_name": audit_table_name,
}

default_args = {
    'owner': f'hca_{lob_abbr}_atos',
    'depends_on_past': False,
    'email': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=480),
}

# =============================== Helpers =========================================
def resolve_target_table(raw_target: str, env: dict) -> str:
    """
    token.dataset pattern: v_psc_stage_dataset_name.Table -> <env value>.Table
    returns "<dataset>.<table>"
    """
    if '.' in raw_target:
        dataset_param, table_name = raw_target.split('.', 1)
        if dataset_param.startswith('v_') and dataset_param in env:
            return f"{env[dataset_param]}.{table_name}"
        else:
            return f"{dataset_param}.{table_name}"
    return raw_target

def parse_group_records(cfg: dict, tbl_grp: str, env: dict):
    """
    Each entry is "~" delimited:
    srctableid~srctablename~tgttablename~tgttableloadtype~srctablequery~rowcommitsize
    """
    rows = []
    for rec in cfg[tbl_grp]:
        parts = rec.split('~', 5)
        if len(parts) != 6:
            raise ValueError(f"Bad tblist row in {tbl_grp}: {rec}")
        srctableid, srctablename, tgttablename, loadtype, srctablequery, rowcommitsize = parts
        sql = re.sub(r'\s+', ' ', srctablequery.strip())
        ds_tbl = resolve_target_table(tgttablename.strip(), env)  # dataset.table
        dataset, table = ds_tbl.split('.', 1)
        rows.append({
            "src_table_id": srctableid.strip(),
            "src_table_name": srctablename.strip(),
            "target_dataset": dataset,
            "target_table": table,
            "load_type": loadtype.strip().lower(),
            "src_query": sql,
            "row_commit_size": int(rowcommitsize.strip()),
        })
    return rows

def fetch_sql_creds_from_secret(secret_url: str) -> str:
    """
    secret_url: projects/<NUM>/secrets/<NAME>/versions/<VER|latest>
    payload expected: user, password
    """
    client = secretmanager.SecretManagerServiceClient()
    resp = client.access_secret_version(name=secret_url)
    payload = resp.payload.data.decode("utf-8")
    data = json.loads(payload)
    return data["user"], data["password"] 
# {"username": data.get("user"), "password": data.get("password")}

def extract_db_from_srctable(srctable: str):
    """
    Extract database name if srctable is like:
      'SSP_Claim_DW.[GCP].vw_...'  -> 'SSP_Claim_DW'
      'EOM_Snapshot.[GCP].vw_...'  -> 'EOM_Snapshot'
    If srctable starts with '[schema]' or just 'schema.table' (no DB prefix), return None.
    """
    s = srctable.strip()
    if not s:
        return None
    # If it starts with [schema]... then no DB prefix
    if s.startswith('['):
        return None
    # Otherwise take the first token before the first dot as DB
    # (handles 'DB.[schema].object' or 'DB.schema.object')
    if '.' in s:
        return s.split('.', 1)[0]
    return None

def read_template(util_folder, filename):
    template = open(os.path.join(util_folder, filename) , 'r')
    return template.read()

def render_template(content_template, dict_instance):
    template_instance = Template(content_template)
    rendered_template = template_instance.render(dict_instance)
    return rendered_template

def pull_last_load_ctrl_timestamp(load_table_name: str, sql_script_file_name: str):
    """
    Extract Last Load Control Timestamp for table
    """
    bq_client = bigquery.Client()

    sql_template = read_template(ingest_dir, sql_script_file_name)

    params.update({
        f"param_load_table_name": load_table_name
    })

    sql_dict = {
    "params":params
    }

    bq_query = render_template(sql_template, sql_dict)

    bq_resultset = bq_client.query(bq_query, project=bq_project_id, location='US').result()
    last_load_ctrl_timestamp = None
    for bq_row in bq_resultset:
        last_load_ctrl_timestamp = bq_row.last_load_ctrl_timestamp
    return {"last_load_ctrl_timestamp": str(last_load_ctrl_timestamp)[:23]}

def update_last_load_ctrl_timestamp(load_table_name: str, sql_script_file_name: str):
    """
    Update Last Load Control Timestamp for table
    """
    bq_client = bigquery.Client()

    sql_template = read_template(ingest_dir, sql_script_file_name)

    params.update({
        f"param_load_table_name": load_table_name
    })

    sql_dict = {
    "params":params
    }

    bq_query = render_template(sql_template, sql_dict)

    for attempt in range(1, 20):
        try:
            bq_resultset = bq_client.query(bq_query, project=bq_project_id, location='US').result()
            break
        except exceptions.BadRequest as exc:
            # examine exc for details and handle the BadRequest error accordingly
            logging.info("data load attempt {} failed with exception {}".format(attempt, str(exc)))
            if ('Too many DML statements outstanding against table'.lower() in str(exc).lower() or 
                'Could not serialize access to table'.lower() in str(exc).lower()):
                # wait_time = (math.ceil(2.5 ** attempt))
                wait_time = random.randint(10,60)
                time.sleep(wait_time)
            else:
                logging.info("Unhandled Exception : {}".format(exc))
                raise SystemExit()
        except exceptions.Forbidden as exc:
            # examine exc for details and handle the Forbidden error accordingly
            logging.info("data load attempt {} failed with exception {}".format(attempt, str(exc)))
            if ('Your table exceeded quota for total number of dml jobs writing to a table'.lower() in str(exc).lower()):
                wait_time = random.randint(10,60)
                time.sleep(wait_time)
            else:
                logging.info("Unhandled Exception : {}".format(exc))
                raise SystemExit()
        except exceptions.InternalServerError as exc:
            # examine exc for details and handle the InternalServerError error accordingly
            logging.info("data load attempt {} failed with exception {}".format(attempt, str(exc)))
            if ('An internal error occurred and the request could not be completed'.lower() in str(exc).lower()):
                wait_time = random.randint(10,60)
                time.sleep(wait_time)
            else:
                logging.info("Unhandled Exception : {}".format(exc))
                raise SystemExit()
        except exceptions.ServiceUnavailable as exc:
            # examine exc for details and handle the ServiceUnavailable error accordingly
            logging.info("data load attempt {} failed with exception {}".format(attempt, str(exc)))
            wait_time = random.randint(10,60)
            time.sleep(wait_time)
        except Exception:
            # handle all other exceptions
            raise SystemExit()

    return bq_resultset.num_dml_affected_rows

def get_record_count(load_table_name: str, sql_script_file_name: str):
    """
    Get Record Count for the Load Table
    """
    bq_client = bigquery.Client()

    sql_template = read_template(ingest_dir, sql_script_file_name)

    params.update({
        f"param_load_table_name": load_table_name
    })

    sql_dict = {
    "params":params
    }

    bq_query = render_template(sql_template, sql_dict)

    bq_resultset = bq_client.query(bq_query, project=bq_project_id, location='US').result()
    load_rec_count = None
    for bq_row in bq_resultset:
        load_rec_count = bq_row.Rec_Count
    return {"load_rec_count": load_rec_count}

def render_audit_query(**kwargs):
    """
    Get Record Count for the Load Table
    """
    # bq_client = bigquery.Client()

    # load_end_time = kwargs['load_end_time']
    sql_script_file_name = kwargs['sql_script_file_name']

    load_start_time = kwargs['load_start_time']
    conv_start_time = datetime.strptime(load_start_time, '%Y-%m-%d %H:%M:%S.%f%z')
    load_start_time_local = current_timezone.convert(conv_start_time)

    load_end_time = kwargs['load_end_time']
    conv_end_time = datetime.strptime(load_end_time, '%Y-%m-%d %H:%M:%S.%f%z')
    load_end_time_local = current_timezone.convert(conv_end_time)

    load_run_time = str(load_end_time_local - load_start_time_local)

    dataflow_job_details = kwargs['dataflow_job_details']

    dataflow_job_details_fmt = dataflow_job_details.replace("'",'"')

    dataflow_job_details_json = json.loads(dataflow_job_details_fmt)

    sql_template = read_template(ingest_dir, sql_script_file_name)

    params.update({
        f"table_id": kwargs['table_id'],
        f"source_sys_name": kwargs['source_sys_name'],
        f"source_table_name": kwargs['source_table_name'],
        f"target_table_name": kwargs['target_table_name'],
        f"actual_value": kwargs['actual_value'],
        f"load_start_time": str(load_start_time_local)[:26],
        f"load_end_time": str(load_end_time_local)[:26],
        f"load_run_time": load_run_time,
        f"source_query": kwargs['source_query'],
        f"job_name": dataflow_job_details_json["name"]
    })

    sql_dict = {
    "params":params
    }

    audit_query = render_template(sql_template, sql_dict)

    audit_query = re.sub(r'\s+', ' ', audit_query.strip())

    return {"audit_query": audit_query}

def slug(s: str) -> str:
    return s.lower().replace("_", "-")

def get_number_from_string(source_string) -> int:
    regexp_subquery = re.compile(r"\d+",re.IGNORECASE)
    source_string_find = regexp_subquery.search(source_string)
    if source_string_find is None:
        number_found = None
    else:
        number_found = source_string_find.group()
    return number_found

# =============================== DAG Factory =====================================
def create_dag(
    dag_id: str,
    schedule: str,
    start_date: str,
    source_system: str,
    server_host: str,
    db_type: str,
    frequency: str,
    tags: list,
    done_file: str,
    tblist: list,
    has_sensor: str,
    sensor_list: list,
    trigger_dag_ids: list,
    schedule_grp: str,
):
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        start_date=eval(start_date),  # your YAML stores pendulum.datetime(...)
        schedule_interval=schedule,
        is_paused_upon_creation=True,
        catchup=False,
        max_active_runs=1,
        max_active_tasks=18,
        concurrency=18,
        tags=tags,
    )

    with dag:
        start_job = DummyOperator(task_id='start_dag')
        end_job = DummyOperator(task_id='end_dag')

        # -------------------- Optional 'done file' sensor & cleanup --------------------
        if (done_file != 'NONE'):
            prev = now = ''
            try:
                if frequency == "daily":
                    now  = pendulum.now(current_timezone).strftime("%Y%m%d")
                    prev = (pendulum.now(current_timezone).subtract(days=1)).strftime("%Y%m%d")
                elif frequency == "weekly":
                    now  = pendulum.yesterday(current_timezone).strftime("%Y%m%d")
                    prev = (pendulum.yesterday(current_timezone).subtract(days=7)).strftime("%Y%m%d")
            except Exception as e:
                logging.info(f"Invalid frequency '{frequency}': {e}")

            done_file_to_delete = done_file.replace('YYYYMMDD', prev)
            done_file_resolved  = done_file.replace('YYYYMMDD', now)

            file_sensor = GCSObjectsWithPrefixExistenceSensor(
                bucket=config['env']['v_data_bucket_name'],
                prefix=f"{config['env']['v_srcfilesdir']}{source_system}/{done_file_resolved}",
                timeout=600,
                mode="reschedule",
                task_id=f"check_{source_system}_done_file_exists",
            )

            delete_old_done_files = PythonOperator(
                task_id=f'delete_{source_system}_old_done_files',
                python_callable=cu.removegcsfileifexists,
                op_kwargs={
                    'sourcesysname': source_system,
                    'folder': config['env']['v_srcfilesdir'],
                    'filename': done_file_to_delete,
                },
            )

        # ----------------------------- Upstream sensors (optional) -----------------------------
        if has_sensor == 'Yes' and sensor_list:
            with TaskGroup(group_id=f'TG-sensor-{source_system}') as sensor_grp:
                for sensor in sensor_list:
                    ext_dag_id  = sensor["dag_id"]
                    ext_task_id = sensor["task_id"]
                    schedule_s  = sensor["schedule"]
                    cycle_age   = sensor.get("cycle_age", "current")
                    ExternalTaskSensor(
                        task_id=f"Check_status_{ext_dag_id}",
                        external_dag_id=ext_dag_id,
                        external_task_id=ext_task_id,
                        timeout=10800,
                        execution_date_fn=cu.get_execution_date,
                        params={"schedule": schedule_s, "frequency": frequency, "cycle_age": cycle_age},
                        allowed_states=["success"],
                        failed_states=["failed", "skipped"],
                        mode="reschedule",
                    )

        # ----------------------------- Secret fetch (SQL creds) -----------------------------
        full_secret_path = f"{config['env']['v_pwd_secrets_url'].rstrip('/')}/{srcsys_config['v_secret_name'].lstrip('/')}"

        cred_user, cred_password = fetch_sql_creds_from_secret(full_secret_path)

        # ----------------------------- Launch Google JDBC -> BQ Flex Template -----------------------------
        template_spec = f"gs://dataflow-templates-{config['env']['v_region']}/latest/flex/Jdbc_to_BigQuery_Flex"
        driver_jars_gcs = f"{config['env']['v_dfjarbucket'].rstrip('/')}/{srcsys_config['v_jdbc_jar']}"

        server_host_no = get_number_from_string(server_host)

        #run parallel df jobs
        with TaskGroup(group_id=f'run_dataflow_jobs_preprocessing') as dataflow_group_preprocessing:
            run_dataflow_job = [BashOperator(
                task_id=f"run_{source_system}",
                dag=dag,
                # to start parallel tasks at random times within 5 minutes, sleep n seconds
                bash_command=f"sleep $(shuf -i 1-30 -n 1) ; python /home/airflow/gcs/dags/{lob}/scripts/{config['env']['v_polling_template']}"
                f" --src_sys_config_file={src_system_file_prefix}_ingest_config.yaml"
                f" --wave_group={schedule_grp} --polling_type=preprocess --server_host_no={server_host_no} --beam_runner=DataflowRunner"
            )]

            wait_for_python_job_async_done = DataflowJobStatusSensor(
                task_id=f"wait_for_python_job_async_done",
                job_id=f"{{{{ task_instance.xcom_pull(task_ids= 'run_dataflow_jobs_preprocessing.run_{source_system}')}}}}",
                expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
                mode="reschedule",
                poke_interval=300,
                location=config['env']['v_region'],
            )

            run_dataflow_job >> wait_for_python_job_async_done

        with TaskGroup(group_id='run_dataflow_jobs') as dataflow_group_all_tables:
            for item in tblist:
                tbl_grp = item["tbl_grp"]

                # Parse group (read SQL + target from YAML)
                group_records = parse_group_records(srcsys_config, tbl_grp, config['env'])
                for rec in group_records:

                    target_table = str(rec['target_table']).lower()
                    output_table = f"{bq_project_id}:{rec['target_dataset']}.{target_table}"
                    
                    with TaskGroup(group_id=f"exec_dataflow_job_{target_table}") as dataflow_group:
                    
                        src_query = rec["src_query"]
                        load_type = rec["load_type"]
                        if load_type == "merge":
                            read_ctrl_table = PythonOperator(
                                task_id=f'read_control_table',
                                python_callable=pull_last_load_ctrl_timestamp,
                                op_kwargs={
                                    'load_table_name': target_table,
                                    'sql_script_file_name': "select_control_timestamp.sql",
                                },
                            )
                            last_load_ctrl_timestamp = f"{{{{ ti.xcom_pull(task_ids='run_dataflow_jobs.exec_dataflow_job_{target_table}.read_control_table')['last_load_ctrl_timestamp'] }}}}"
                            src_query = src_query.replace("v_last_load_ctrl_timestamp", str(last_load_ctrl_timestamp))

                        # Build a hyphen-safe job name prefix (don't touch the macro)
                        _prefix = f"jdbc-to-bq-{slug(source_system)}-{slug(schedule_grp)}-{slug(target_table)}"
                        audit_prefix = f"audit-insert-{slug(source_system)}-{slug(schedule_grp)}-{slug(target_table)}"

                        # derive DB name dynamically from srctablename ===
                        db_name = extract_db_from_srctable(rec["src_table_name"])
                        base_url = f"jdbc:sqlserver://{server_host}:{srcsys_config['v_port']};"
                        conn_props = []
                        if db_name:
                            conn_props.append(f"databaseName={db_name}")
                        conn_props.append("encrypt=true")
                        conn_props.append("trustServerCertificate=true")
                        connection_url = base_url + (";" if conn_props else "") + ";".join(conn_props)

                        load_big_query_table = DataflowStartFlexTemplateOperator(
                            task_id=f"jdbc_to_bigquery_load",
                            project_id=config['env']['v_proc_project_id'],
                            location=config['env']['v_region'],  # us-east4
                            body={
                                "launch_parameter": {
                                    "jobName": f"{_prefix}-{{{{ ds_nodash }}}}",
                                    "containerSpecGcsPath": template_spec,
                                    "parameters": {
                                        # JDBC
                                        "driverJars": driver_jars_gcs,
                                        "driverClassName": srcsys_config['v_jdbc_class_name'],
                                        "connectionURL": connection_url,
                                        "username": cred_user,
                                        "password": cred_password,
                                        "bigQueryLoadingTemporaryDirectory": config['env']['v_gcs_temp_bucket'],
                                        # Behavior
                                        "fetchSize": str(rec["row_commit_size"]),
                                        f"isTruncate": "true",
                                        # Query mode
                                        f"query": src_query,
                                        # Output (BQ)
                                        f"outputTable": output_table
                                    },
                                    "environment": {
                                        "serviceAccountEmail": config['env']['v_serviceaccountemail'],
                                        "tempLocation":  config['env']['v_gcs_temp_bucket'],
                                        "network": config['env'].get('v_network'),
                                        "subnetwork": config['env'].get('v_subnetwork'),
                                        "ipConfiguration": "WORKER_IP_PRIVATE",
                                        "numWorkers": int(config['env'].get('v_numworkers', 1)),
                                        "maxWorkers": int(config['env'].get('v_maxworkers', 2)),
                                        "machineType": srcsys_config.get('v_machine_type', config['env'].get('v_machinetype', 'n2-standard-8')),
                                    },
                                }
                            },
                            wait_until_finished=True,
                            drain_pipeline=False,
                        )

                        get_load_count = PythonOperator(
                            task_id=f'get_load_count',
                            python_callable=get_record_count,
                            op_kwargs={
                                'load_table_name': target_table,
                                'sql_script_file_name': "select_load_count.sql",
                            },
                        )

                        build_audit_query = PythonOperator(
                            task_id=f'build_audit_query',
                            python_callable=render_audit_query,
                            # provide_context=True,
                            op_kwargs={
                                'table_id': rec['src_table_id'],
                                'source_sys_name': source_system,
                                'source_table_name': rec["src_table_name"],
                                'target_table_name': f"{rec['target_dataset']}.{target_table}",
                                'actual_value': f"{{{{ ti.xcom_pull(task_ids='run_dataflow_jobs.exec_dataflow_job_{target_table}.get_load_count')['load_rec_count'] }}}}",
                                'load_start_time': f"{{{{ dag_run.get_task_instance('run_dataflow_jobs.exec_dataflow_job_{target_table}.jdbc_to_bigquery_load').start_date }}}}",
                                'load_end_time': f"{{{{ dag_run.get_task_instance('run_dataflow_jobs.exec_dataflow_job_{target_table}.get_load_count').start_date }}}}",
                                'source_query': src_query,
                                'sql_script_file_name': "select_audit_values_sqlserver.sql",
                                'dataflow_job_details': f"{{{{ ti.xcom_pull(task_ids='run_dataflow_jobs.exec_dataflow_job_{target_table}.jdbc_to_bigquery_load') }}}}",
                            },
                        )

                        insert_into_audit_table = DataflowStartFlexTemplateOperator(
                            task_id=f"insert_into_audit",
                            project_id=config['env']['v_proc_project_id'],
                            location=config['env']['v_region'],  # us-east4
                            body={
                                "launch_parameter": {
                                    "jobName": f"{audit_prefix}-{{{{ ds_nodash }}}}",
                                    "containerSpecGcsPath": template_spec,
                                    "parameters": {
                                        # JDBC
                                        "driverJars": driver_jars_gcs,
                                        "driverClassName": srcsys_config['v_jdbc_class_name'],
                                        "connectionURL": connection_url,
                                        "username": cred_user,
                                        "password": cred_password,
                                        "bigQueryLoadingTemporaryDirectory": config['env']['v_gcs_temp_bucket'],
                                        # Behavior
                                        "fetchSize": str(rec["row_commit_size"]),
                                        f"isTruncate": "false",
                                        # Query mode
                                        f"query": f"{{{{ ti.xcom_pull(task_ids='run_dataflow_jobs.exec_dataflow_job_{target_table}.build_audit_query')['audit_query'] }}}}",
                                        # Output (BQ)
                                        f"outputTable": f"{bq_project_id}.{audit_dataset}.{audit_table_name}"
                                    },
                                    "environment": {
                                        "serviceAccountEmail": config['env']['v_serviceaccountemail'],
                                        "tempLocation":  config['env']['v_gcs_temp_bucket'],
                                        "network": config['env'].get('v_network'),
                                        "subnetwork": config['env'].get('v_subnetwork'),
                                        "ipConfiguration": "WORKER_IP_PRIVATE",
                                        "numWorkers": int(config['env'].get('v_numworkers', 1)),
                                        "maxWorkers": int(config['env'].get('v_maxworkers', 2)),
                                        "machineType": srcsys_config.get('v_machine_type', config['env'].get('v_machinetype', 'n2-standard-8')),
                                    },
                                }
                            },
                            wait_until_finished=True,
                            drain_pipeline=False,
                        )

                        if load_type == "merge":
                            update_ctrl_table = PythonOperator(
                                task_id=f'update_control_table',
                                python_callable=update_last_load_ctrl_timestamp,
                                op_kwargs={
                                    'load_table_name': target_table,
                                    'sql_script_file_name': "update_control_timestamp.sql",
                                },
                            )
                            update_ctrl_table >> read_ctrl_table >> load_big_query_table >> get_load_count >> build_audit_query >> insert_into_audit_table
                        else:
                            load_big_query_table >> get_load_count >> build_audit_query >> insert_into_audit_table

                    dataflow_group

        # ----------------------------- Optional downstream triggers -----------------------------
        if trigger_dag_ids:
            with TaskGroup(group_id='TG-trigger_dag') as trigger_dag_grp:
                for trigger_dag_id in trigger_dag_ids:
                    TriggerDagRunOperator(
                        task_id=f"trigger_dag_{trigger_dag_id}",
                        trigger_dag_id=trigger_dag_id,
                    )

        if trigger_dag_ids:
            if (done_file != 'NONE'):
                if has_sensor == 'Yes':
                    start_job >> dataflow_group_preprocessing >> file_sensor >> sensor_grp >> dataflow_group_all_tables >> delete_old_done_files >> trigger_dag_grp >> end_job
                else:
                    start_job >> dataflow_group_preprocessing >> file_sensor >> dataflow_group_all_tables >> delete_old_done_files >> trigger_dag_grp >> end_job
            else:
                if has_sensor == 'Yes':
                    start_job >> sensor_grp >> dataflow_group_all_tables >> trigger_dag_grp >> end_job
                else:
                    start_job >> dataflow_group_all_tables >> trigger_dag_grp >> end_job
        else:
            if (done_file != 'NONE'):
                if has_sensor == 'Yes':
                    start_job >> dataflow_group_preprocessing >> file_sensor >> sensor_grp >> dataflow_group_all_tables >> delete_old_done_files >> end_job
                else:
                    start_job >> dataflow_group_preprocessing >> file_sensor >> dataflow_group_all_tables >> delete_old_done_files >> end_job
            else:
                if has_sensor == 'Yes':
                    start_job >> sensor_grp >> dataflow_group_all_tables >> end_job
                else:
                    start_job >> dataflow_group_all_tables >> end_job

    return dag

# =============================== DAG Instances ===================================
source_system = srcsys_config['v_sourcesysnm']
db_type       = srcsys_config['v_databasetype']
schedule_grp  = srcsys_config['v_schedule_grp']

for schedule in srcsys_config['schedule']:
    frequency         = schedule["frequency"]
    schedule_interval = schedule["schedule_interval"]
    start_date        = schedule["start_date"]
    dag_name_suffix   = schedule["dag_name_suffix"]
    done_file         = schedule["done_file"]
    tblist            = schedule["tblist"]
    has_sensor        = schedule["has_sensor"]
    src_config_server = schedule["server"]
    server_host       = config['env'][f'v_{src_config_server}']
    trigger_dag_ids   = schedule["trigger_dag_ids"]
    sensor_list       = schedule.get("sensor")

    schedule_cron = None if schedule_interval == "None" else schedule_interval
    tags = [source_system, schedule_grp, dag_name_suffix]

    if frequency == "None":
        dag_id = f"dag_ingest_{source_system}_{dag_name_suffix}_{db_type}_adhoc"
        tags.extend(['adhoc', lob_abbr])
    else:
        dag_id = f"dag_ingest_{source_system}_{dag_name_suffix}_{db_type}_{frequency}"
        tags.extend([frequency, lob_abbr])

    globals()[dag_id] = create_dag(
        dag_id=dag_id,
        schedule=schedule_cron,
        start_date=start_date,
        source_system=source_system,
        server_host=server_host,
        db_type=db_type,
        frequency=frequency,
        tags=tags,
        done_file=done_file,
        tblist=tblist,
        has_sensor=has_sensor,
        sensor_list=sensor_list,
        trigger_dag_ids=trigger_dag_ids,
        schedule_grp=schedule_grp,
    )