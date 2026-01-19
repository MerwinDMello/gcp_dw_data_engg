import ast
from datetime import datetime, timedelta
import logging
import os
import sys
from typing import List

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, "..", "utils")
sys.path.append(util_dir)

# Import common_utilities after setting the prefix for utils
import common_utilities as cu
from google.cloud import storage

config_cr = cu.call_config_yaml("cr_config.yaml", "hca_cr_default_vars")

bq_project_id = config_cr["env"]["v_curated_project_id"]
clinical_project_id = config_cr["env"]["v_clinical_project_id"]
env_name = config_cr["env"]["v_env_name"]
financial_project_id = config_cr["env"]["v_financial_project_id"]
mirroring_project_id = config_cr["env"]["v_mirroring_project_id"]
psg_project_id = config_cr["env"]["v_psg_project_id"]


def get_sql_file_list(src_folder_path: str) -> List[str]:
    """
    Get list of .sql DDLs to execute.

    :param src_folder_path: GCS prefix

    :returns: List of .sql blobs
    """
    try:
        bucket = cu.get_bucket(
            config_cr["env"]["v_curated_project_id"],
            config_cr["env"]["v_dag_bucket_name"],
        )
        storage_client = storage.Client()
        blobs = storage_client.list_blobs(
            bucket, prefix=src_folder_path, delimiter=None
        )
        blob_list = []

        for blob in blobs:
            if blob.name != src_folder_path:
                blob_list.append(blob.name)

        return blob_list

    except Exception as gex:
        logging.error(f"ERROR retrieving list of .sql DDLs - {gex}")
        raise


default_args = {
    "depends_on_past": False,
    # "email": config_cr["env"]["v_failure_email_list"],
    "email_on_success": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "execution_timeout": timedelta(minutes=60),
    "owner": "hca_cr_atos",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
    "params": {
        "param_auth_base_views_dataset_name": f"{bq_project_id}.{config_cr['env']['v_auth_base_views_dataset_name']}",
        "param_clinical_dataset_name": f"{clinical_project_id}.{config_cr['env']['v_auth_base_views_dataset_name']}",
        "param_cp_base_views_dataset_name": f"{bq_project_id}.{config_cr['env']['v_cp_base_views_dataset_name']}",
        "param_cr_audit_dataset_name": f"{bq_project_id}.{config_cr['env']['v_cr_audit_dataset_name']}",
        "param_cr_base_views_dataset_name": f"{bq_project_id}.{config_cr['env']['v_cr_base_views_dataset_name']}",
        "param_cr_bi_views_dataset_name": f"{bq_project_id}.{config_cr['env']['v_cr_bi_views_dataset_name']}",
        "param_cr_core_dataset_name": f"{bq_project_id}.{config_cr['env']['v_cr_core_dataset_name']}",
        "param_cr_procs_dataset_name": f"{bq_project_id}.{config_cr['env']['v_cr_procs_dataset_name']}",
        "param_cr_stage_dataset_name": f"{bq_project_id}.{config_cr['env']['v_cr_stage_dataset_name']}",
        "param_cr_views_dataset_name": f"{bq_project_id}.{config_cr['env']['v_cr_views_dataset_name']}",
        "param_edw_sec_base_views_dataset_name": f"{bq_project_id}.{config_cr['env']['v_sec_base_views_dataset_name']}",
        "param_financials_dataset_name": f"{financial_project_id}.{config_cr['env']['v_auth_base_views_dataset_name']}",
        "param_fns_dataset_name": f"{bq_project_id}.{config_cr['env']['v_bqutil_fns_dataset_name']}",
        "param_mirroring_dataset_name": f"{mirroring_project_id}.{config_cr['env']['v_cr_mirrored_core_dataset_name']}",
        "param_pf_base_views_dataset_name": f"{bq_project_id}.{config_cr['env']['v_pf_base_views_dataset_name']}",
        "param_pf_staging_dataset_name": f"{bq_project_id}.{config_cr['env']['v_pf_staging_dataset_name']}",
        "param_psg_dataset_name": f"{psg_project_id}.{config_cr['env']['v_auth_base_views_dataset_name']}",
        "param_pub_dataset_name": f"{bq_project_id}.{config_cr['env']['v_pub_views_dataset_name']}",
        "param_sec_core_dataset_name": f"{bq_project_id}.{config_cr['env']['v_sec_core_dataset_name']}",
    },
}


with DAG(
    catchup=False,
    dag_id="dag_create_tables_from_ddl_cr",
    default_args=default_args,
    is_paused_upon_creation=True,
    max_active_runs=1,
    max_active_tasks=10,
    schedule_interval=None,
    tags=["cr"],
    template_searchpath="/home/airflow/gcs/",
) as dag:
    start_job = DummyOperator(task_id="start_job")
    end_job = DummyOperator(task_id="end_job")
    folder_list = ast.literal_eval(Variable.get("cr_var_dataset"))

    arr = []
    for folder in folder_list:
        with TaskGroup(group_id=f"{folder}") as folder_grp:
            blob_list = get_sql_file_list(f"dags/edwcr/sql/ddl/{folder}/")

            for blob in blob_list:
                dataset = blob.split("/")[4]
                blob_obj = blob.split("/")[5].split(".")[0]
                run_sql_ddl = BigQueryOperator(
                    gcp_conn_id=config_cr["env"]["v_curated_conn_id"],
                    sql=blob,
                    use_legacy_sql=False,
                    retries=0,
                    task_id=f"bqsql_create_{dataset}_{blob_obj}",
                )
            [run_sql_ddl]

        start_job >> folder_grp >> end_job
