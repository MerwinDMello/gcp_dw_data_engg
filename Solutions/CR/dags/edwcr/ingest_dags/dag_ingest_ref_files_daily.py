from datetime import datetime, timedelta, timezone
import logging
import os
import sys
from typing import List, TypedDict
import uuid

from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery, storage
from google.cloud.exceptions import BadRequest, Forbidden, NotFound
import pendulum
import yaml


script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, "..", "utils")
sys.path.append(util_dir)

EDWCR_DIR = os.path.dirname(script_dir)
config_file = f"{EDWCR_DIR}/config/cr_ref_file_config.yaml"
with open(config_file, "r") as file:
    REF_CONFIG = yaml.safe_load(file)
import common_utilities as cu

CR_CONFIG = cu.call_config_yaml("cr_config.yaml", "hca_cr_default_vars")
SOURCE_FILES_PROJECT = CR_CONFIG["env"]["v_landing_project_id"]
SOURCE_FILES_BUCKET = CR_CONFIG["env"]["v_source_file_bucket"]
SOURCE_BLOBS = REF_CONFIG["SOURCE_BLOBS"]

BQ_PROJECT = CR_CONFIG["env"]["v_curated_project_id"]
BQ_STG_DATASET = f"{CR_CONFIG['env']['v_cr_stage_dataset_name']}"
BQ_CORE_DATASET = f"{CR_CONFIG['env']['v_cr_core_dataset_name']}"

AUDIT_DATASET = CR_CONFIG["env"]["v_cr_audit_dataset_name"]
AUDIT_TABLE = CR_CONFIG["env"]["v_audittablename"]

current_timezone = pendulum.timezone("US/Central")
exe_time = pendulum.now(current_timezone)

default_args = {
    "email": "email",
    "email_on_failure": False,
    "email_on_retry": False,
    "email_on_success": False,
    "execution_timeout": timedelta(minutes=30),
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
    "start_date": "2024-01-01T00:00:01",
}

dag = DAG(
    is_paused_upon_creation=True,
    catchup=False,
    concurrency=1,
    dag_id="dag_ingest_ref_files_daily",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="0 1 * * *",
    template_searchpath="/home/airflow/gcs/dags/",
    tags=["cr", "daily", "ingest"],
)


class BlobMeta(TypedDict):
    name: str
    path: str
    updated: datetime


def get_object_metadata(source_files: List[str]) -> List[BlobMeta]:
    """
    Get the metadata for the objects in the source bucket.

    :param source_files: List of blob names to filter on.

    :returns: List of dicts with reference file blob metadata
    """
    try:
        storage_client = storage.Client(project=SOURCE_FILES_PROJECT)
        source_bucket = storage_client.bucket(SOURCE_FILES_BUCKET)
        blobs = storage_client.list_blobs(source_bucket)

        blobs_metadata = []

        for blob in blobs:
            if blob.name.lower() in [ref.lower() for ref in source_files]:
                blobs_metadata.append(
                    {
                        "name": blob.name,
                        "path": f"gs://{SOURCE_FILES_BUCKET}/{blob.name}",
                        "updated": blob.updated,
                    }
                )

        if not blobs_metadata:
            logging.error(
                f"Files not found in {SOURCE_FILES_PROJECT} gs://{SOURCE_FILES_BUCKET} "
            )
            raise ValueError("Source blobs missing")
        return blobs_metadata

    except Exception as gex:
        print(f"EXCEPTION getting object metadata - {gex}")
        raise


def check_object_metadata(
    blobs_meta_dict: List[BlobMeta], prev_dag_run_ts: pendulum.datetime
) -> List[BlobMeta]:
    """
    Check when a blob was last updated relative to a given timestamp.

    :param blobs_meta_dict: Dictionary of blobs to check
    :param prev_dag_run_ts: Timestamp to check blob update time against

    :returns: List of blobs to reload from Cloud Storage to BigQuery
    """

    try:
        blobs_to_reload = []
        for blob in blobs_meta_dict:

            if blob.get("updated") >= prev_dag_run_ts:
                logging.info(
                    f"{blob.get('name')} was updated on {blob.get('updated')} which is more recent than the previous check on {prev_dag_run_ts}"
                )
                blobs_to_reload.append(blob)
            else:
                logging.info(
                    f"{blob.get('name')} was updated on {blob.get('updated')} which is less recent than the previous check on {prev_dag_run_ts}"
                )

        return blobs_to_reload

    except Exception as gex:
        logging.error(
            f"EXCEPTION listing objects in {SOURCE_FILES_PROJECT} gs://{SOURCE_FILES_BUCKET} - {gex}"
        )
        raise


def load_gcs_to_bq(
    bq_client_inst: bigquery.Client,
    gcs_path: str,
    bq_project: str,
    bq_dataset: str,
    bq_table: str,
    blob_format: str = bigquery.SourceFormat.CSV,
    write_disposition: str = bigquery.WriteDisposition.WRITE_TRUNCATE,
    bq_schema: List[bigquery.SchemaField] = None,
    autodetect: bool = False,
    time_partitioning: str = None,
    skip_leading_rows: int = None,
    location: str = None,
    use_avro_logical_types: bool = False,
    allow_jagged_rows: bool = False,
    delimiter: str = "|",
):
    """
    Load data from one or more Cloud Storage blobs to a BigQuery table.

    :param bq_client_inst: BigQuery client object
    :param gcs_path: GCS object prefix
    :param bq_project: BigQuery project ID
    :param bq_dataset: BigQuery dataset ID
    :param bq_table: BigQuery table ID
    :param blob_format: File format
    :param write_disposition: BigQuery write mode
    :param bq_schema: Destination BigQuery table schema
    :param autodetect: Whether to use schema autodetection
    :param time_partitioning: Time partitioning field
    :param skip_leading_rows: Number of rows to skip
    :param location: Location of destination dataset
    :param use_avro_logical_types: Whether to se AVRO's logical types
    :param allow_jagged_rows: Allow missing trailing optional columns (CSV only)
    :param delimiter: Delimiting CSV character
    """
    full_table_id = f"{bq_project}.{bq_dataset}.{bq_table}"
    logging.info(f"Loading `{full_table_id}` from {gcs_path}")

    start_time = datetime.now(timezone.utc)
    job_config = bigquery.LoadJobConfig()
    job_config.field_delimiter = delimiter

    if bq_schema:
        job_config.schema = bq_schema

    if time_partitioning:
        job_config.time_partitioning = time_partitioning

    if skip_leading_rows:
        job_config.skip_leading_rows = skip_leading_rows

    job_config.autodetect = autodetect
    job_config.source_format = blob_format
    job_config.write_disposition = write_disposition
    job_config.use_avro_logical_types = use_avro_logical_types
    job_config.allow_jagged_rows = allow_jagged_rows

    bq_client_inst.get_table(full_table_id)
    logging.info(bq_client_inst.get_table(full_table_id))

    load_job = None

    try:
        load_job = bq_client_inst.load_table_from_uri(
            gcs_path,
            full_table_id,
            location=location,
            job_config=job_config,
        )
        load_job.result()
        end_time = datetime.now(timezone.utc)
        logging.info(f"Loading data into BigQuery took {end_time - start_time}")

    except (BadRequest, Forbidden, NotFound) as bqex:
        logging.error(
            f"Job {getattr(load_job, 'job_id', None)} failed with exception {bqex} because {getattr(bqex, 'errors', None)}"
        )
        raise

    except Exception as gex:
        logging.error(f"Other EXCEPTION loading objects into BigQuery - {gex}")
        raise


def run_bq_query(
    bq_client_inst: bigquery.Client,
    query_string: str,
    return_result: bool = False,
    table_name: str = None,
):
    """
    Run a query job in BigQuery.

    :param bq_client_inst: BigQuery client instance
    :param query_string: BigQuery query statement to run
    :param return_result: Boolean parameter to return the BigQuery query reeult
    :param table_name: BigQuery table object to retrieve

    :returns: BigQuery query job results
    """
    try:
        if table_name:
            table = bq_client_inst.get_table(table_name)
            return table

        query_job = bq_client_inst.query(query_string)

        logging.info(f"Started query job: {query_job.job_id}")
        for status in query_job.result():
            logging.info(status)

        if return_result:
            result = query_job.result()
            return result

    except (BadRequest, Forbidden, NotFound) as bqex:
        logging.error(
            f"Job {getattr(query_job, 'job_id', None)} failed with exception {bqex} because {getattr(bqex, 'errors', None)}"
        )
        raise

    except Exception as gex:
        logging.error(f"Other EXCEPTION running query statement in BigQuery - {gex}")
        raise


def run_bq_insert(src_table_name: str, trg_table_name: str,dag_id :str):
    """
    Execute the flow to enter excution date in audit table.

    :param src_table_name: source table name
    :param trg_table: target table name
    :param dag_id: dag id and run id from airflow
    """
    try:
        source_table_name = src_table_name
        load_start_time = exe_time
        load_end_time = pendulum.now(current_timezone)
        target_table = trg_table_name
        bq_client = bigquery.Client(project=BQ_PROJECT)
        dag_id= dag_id
        audit_records = []
        audit_records.append(
            {
                "uuid": str(uuid.uuid4()),
                "table_id": None,
                "src_sys_nm": "cr",
                "src_tbl_nm": source_table_name,
                "tgt_tbl_nm": target_table,
                "audit_type": "Execution Time",
                "expected_value": None,
                "actual_value": None,
                "load_start_time": load_start_time,
                "load_end_time": load_end_time,
                "load_run_time": str(
                    datetime.fromisoformat(load_end_time.to_datetime_string())
                    - datetime.fromisoformat(load_start_time.to_datetime_string())
                ),
                "job_name": dag_id,
                "audit_time": pendulum.now(current_timezone),
                "audit_status": "Pass",
            }
        )

        audit_table_ref = bq_client.get_table(AUDIT_TABLE)
        bq_client.insert_rows(audit_table_ref, audit_records)

    except Exception as gex:
        logging.error(f"EXCEPTION inserting audit data - {gex}")
        raise


def run_flat_file_checks(**kwargs):
    """
    Execute the flow to check whether a given ref file needs to be reloaded.
    """
    try:
        bq_client = bigquery.Client(project=BQ_PROJECT)
        dagid=kwargs.get("dag").dag_id+'-'+kwargs.get("run_id")
        audit_query = f"""
            SELECT COALESCE(MAX(load_start_time),'1999-01-01T00:00:00.000000') load_start_time
            FROM `{BQ_PROJECT}`.{AUDIT_DATASET}.audit_control
            WHERE tgt_tbl_nm IN ('edwcr.ref_lookup_name', 'edwcr.ref_navigation_measure')
            AND audit_type='Execution Time'
        """
        audit_result = run_bq_query(bq_client, audit_query, return_result=True)

        for row in audit_result:
            audit_time = row[0].astimezone()

        blobs_metadata = get_object_metadata(SOURCE_BLOBS)
        blobs_to_reload = check_object_metadata(blobs_metadata, audit_time)

        if blobs_to_reload:
            for blob in blobs_to_reload:
                s_file = f"{blob.get('name')}"
                table_name = REF_CONFIG[s_file]["stage_table_name"]
                target_table_name = REF_CONFIG[s_file]["target_table_name"]

                live_schema = run_bq_query(
                    bq_client,
                    query_string="",
                    return_result=False,
                    table_name=f"{BQ_STG_DATASET}.{table_name}",
                )

                schema = live_schema.schema
                load_gcs_to_bq(
                    bq_client,
                    blob.get("path"),
                    BQ_PROJECT,
                    BQ_STG_DATASET,
                    table_name,
                    bq_schema=schema,
                )

                sql_file_path = ""
                sys.path.append(EDWCR_DIR)
                sql_file_path = f"{EDWCR_DIR}/sql/dml/ingest/{target_table_name}.sql"
                query_statement = ""
                with open(sql_file_path, "r") as f:
                    query_statement = f.read()
                    query_statement = query_statement.format(
                        core=BQ_CORE_DATASET, stage=BQ_STG_DATASET
                    )
                run_bq_query(bq_client, query_statement)
                run_bq_insert(
                    src_table_name=f"{BQ_STG_DATASET}.{table_name}",
                    trg_table_name=f"{BQ_CORE_DATASET}.{target_table_name}",
                    dag_id=dagid
                )

        else:
            logging.info("Did not detect any blobs to reload.")

    except Exception as gex:
        logging.error(
            f"EXCEPTION running flat file checks in run_flat_file_checks - {gex}"
        )
        raise


with dag:
    ingest_ref_files = PythonOperator(
        task_id="ingest_ref_files",
        python_callable=run_flat_file_checks,
        provide_context=True,
    )

    ingest_ref_files
