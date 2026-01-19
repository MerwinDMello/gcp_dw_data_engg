from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from airflow.exceptions import AirflowException
import os, sys, re, json, logging
import pendulum

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

current_timezone = pendulum.timezone("US/Central")

gcp_conn_id   = config['env']['v_curated_conn_id']
bq_project_id = config['env']['v_curated_project_id']
core_dataset = config['env'][f'v_{lob_abbr}_core_dataset_name']
audit_dataset = config['env'][f'v_{lob_abbr}_audit_dataset_name']
control_table_name = config['env']['v_controltablename']

MASTER_TABLE = f"{bq_project_id}.{audit_dataset}.{control_table_name}"#"hca-hin-dev-cur-parallon.edwpsc_ac.test_qa"
SOURCE_PREFIX = f"{bq_project_id}.{core_dataset}" #"hca-hin-dev-cur-parallon.edwpsc"

def update_ctrl_timestamps(**context):
    client = bigquery.Client()
    dag_run = context["dag_run"]

    tables = dag_run.conf.get("tables")
    if not tables:
        raise AirflowException("No tables passed in DagRun config!")

    failed_tables = []

    for table in tables:
        print(table)
        try:
            src = f"`{SOURCE_PREFIX}.{table}`"
            sql = f"""
            UPDATE `{MASTER_TABLE}`
            SET last_load_ctrl_timestamp = (
                SELECT MAX(dwlastupdatedatetime) FROM {src}
            )
            WHERE table_name = '{table}'
            """
            client.query(sql).result()
            print(sql)
            print(f"âœ… Updated timestamp for {table}")

        except Exception as e:
            failed_tables.append(table)
            print(f"Failed update for {table}: {e}")

    if failed_tables:
        raise AirflowException(f"Tables not updated: {failed_tables}")

with DAG(
    dag_id="dag_update_load_ctrl_timestamp",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["bq","control-table"]
) as dag:

    update_task = PythonOperator(
        task_id="update_ctrl_timestamp",
        python_callable=update_ctrl_timestamps,
        provide_context=True
    )

    update_task