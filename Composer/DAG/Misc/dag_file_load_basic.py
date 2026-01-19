from airflow import DAG

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_to_bq import GCSToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_gcs import GCSToGCSOperator
from utils import common_utilities as cu

from datetime import datetime, timedelta
import pendulum 
current_timezone = "US/Central"
config = cu.call_config_yaml("hrg_config.yaml","hca_hrg_default_vars")

default_args = {
    'owner': 'hca_hrg_atos',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2023, 3, 17, tz=current_timezone),
    'email': config['env']['v_failure_email_list'],
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=240),
    #params to substitute in sql files
    'params': {
        "param_hr_stage_dataset_name": config['env']['v_curated_project_id'] + 
                                    '.' +  config['env']['v_hr_stage_dataset_name'],
        "param_hr_core_dataset_name": config['env']['v_curated_project_id'] + 
                                    '.' + config['env']['v_hr_core_dataset_name'],
        "param_hr_base_views_dataset_name": config['env']['v_curated_project_id'] + 
                                    '.' + config['env']['v_hr_base_views_dataset_name'],
        "param_hr_views_dataset_name": config['env']['v_curated_project_id'] + 
                                    '.' + config['env']['v_hr_views_dataset_name'],
        "param_pub_views_dataset_name": config['env']['v_curated_project_id'] + 
                                    '.' + config['env']['v_pub_views_dataset_name'],
        }
}

with DAG(
    "dag_ingest_enwisen_audit_files_daily", 
    default_args=default_args, 
    schedule_interval="0 2 * * *", 
    catchup=False, 
    max_active_runs=1, 
    template_searchpath='/home/airflow/gcs/dags/sql/',
    tags=["enwisen"]
) as dag:

    bqdatasetfullname = config['env']['v_curated_project_id'] + ':' + config['env']['v_hr_stage_dataset_name'] +  '.enwisen_audit_wrk'
    bqschemaobjname = config['env']['v_hr_stage_dataset_name'] +  '.' + "enwisen_audit.json"
    gcsbqschemaobjtempfolder = "gs://"  + config['env']['v_data_bucket_name']  + "/edwhrdata/tmp"
    source_system = 'enwisen'
    intermediate_path = ''
    file_name = 'Onboarding_Audit'
    file_extension = 'txt'
    source_file_path = source_system + '/' + intermediate_path + file_name + '.' + file_extension
    archive_file_path = source_system + '/' + intermediate_path + file_name + '_' + \
    datetime.now(pendulum.timezone(current_timezone)).strftime('%Y%m%d') + '.' + file_extension

    #define tasks 
    start_job = DummyOperator(task_id='start_dag')
    end_job = DummyOperator(task_id='end_dag')

    get_table_ddl_enwisen_audit = BashOperator(
        task_id='get_table_ddl_enwisen_audit',
        #run bq to get json formatted schema_object 
        #run gsutil copy  schema_object  to GCS /dags/utils/temp/ folder
        #remove after gsutil cp
        bash_command="bq show --schema --format=json  " + bqdatasetfullname + " > "  + bqschemaobjname
                                + " && gsutil cp "  + bqschemaobjname + " " + gcsbqschemaobjtempfolder
                                + " && rm " + bqschemaobjname
    )

    load_gcsfile_tobq_enwisen_audit=GCSToBigQueryOperator(
        task_id='load_gcsfile_tobq_enwisen_audit',
        bucket=config['env']['v_data_bucket_name'],
        gcp_conn_id=config['env']['v_curated_conn_id'],
        source_objects=config['env']['v_srcfilesdir']  + source_file_path,
        destination_project_dataset_table=config['env']['v_curated_project_id'] + 
                                    '.' + config['env']['v_hr_stage_dataset_name'] + 
                                    '.enwisen_audit_wrk',
        write_disposition='WRITE_TRUNCATE',
        field_delimiter=',',
        quote_character='"',
        autodetect=False,
        skip_leading_rows=1,         
        #using schema object generated and saved in GCS
        schema_object='edwhrdata/tmp/' + bqschemaobjname
    )

    update_bq_table_enwisen_audit=BigQueryOperator(
		task_id='update_bq_table_enwisen_audit',
		gcp_conn_id=config['env']['v_curated_conn_id'],
		use_legacy_sql=False, 
        retries=0,
		sql='dml/ingest/hdw_enwisen_audit_stg.sql'
	)

    archive_srcfile_enwisen_audit=GCSToGCSOperator(
        task_id='archive_srcfile_enwisen_audit',
        source_bucket=config['env']['v_data_bucket_name'],
        destination_bucket=config['env']['v_data_archive_bucket_name'],
        source_object=config['env']['v_srcfilesdir']  + source_file_path,
        destination_object=config['env']['v_archivedir'] + archive_file_path,
        move_object=True        
    )

#setting dag dependency
start_job >> get_table_ddl_enwisen_audit >> load_gcsfile_tobq_enwisen_audit >> update_bq_table_enwisen_audit >> archive_srcfile_enwisen_audit >> end_job