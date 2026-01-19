from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from datetime import datetime, timedelta
import os
from airflow.utils.task_group import TaskGroup
import pendulum
import sys

script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, '..', 'utils')
sys.path.append(util_dir)

import common_utilities as cu

config_name = "ra_config.yaml"
config = cu.call_config_yaml("ra_config.yaml","hca_ra_default_vars")
 # To allow for concurrent runs of new BQ process and old process, certain tables/files have _BQ suffix
bq_suffix =  config['env']['v_bq_suffix']

timezone_str = "US/Central"
timezone = pendulum.timezone(timezone_str)


default_args = {
    'owner': 'hca_clinical_atos',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2022, 12, 8, tz=timezone),
    'email': config['env']['v_failure_email_list'],
    'email_on_success': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=240),
    # params to substitute in sql files
    'params': {
        "param_parallon_ra_stage_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_parallon_ra_stage_dataset_name'],
        "param_parallon_ra_core_dataset_name": config['env']['v_curated_project_id'] + '.' + config['env']['v_parallon_ra_core_dataset_name'],
        "param_parallon_ra_data_bucket_name": config['env']['v_data_bucket_name'],
        "param_parallon_ra_outbound_file_path": config['env']['v_outbound_file_path'],
        "timezone_str": timezone_str,
        "param_parallon_cur_project_id": config['env']['v_curated_project_id']
    }
}


denials_dict = ["American SSC","National SSC","Atlantic SSC","Specialty Services"]

inventory_dict = ["American SSC","National SSC","Atlantic SSC","Specialty Services"]

custom_conditions_arr = [
    {"ssc_name": "AtlanticClient",
     "filter_column": "unit_num",
     "filter_value_list": ["18045","18500","76009"]}
]
ext_dag_id = 'dag_integrate_ra_core_tables_daily_05.15'

dag_id = 'dag_ra_oracle_denials_and_inventory_extract'
dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    start_date=eval("pendulum.datetime(2022, 12, 20, tz=timezone)"),
    schedule_interval="20 6 * * *",
    catchup=False,
    max_active_runs=1,
    template_searchpath='/home/airflow/gcs/dags/edwra/sql/',
    tags=["ra", "daily"]
)
with dag:

    start_job = DummyOperator(task_id='start_dag')
    end_job = DummyOperator(task_id='end_dag')
    check_task_completion= ExternalTaskSensor(
        task_id=f"Check_status_{ext_dag_id}",
        external_dag_id=ext_dag_id,
        external_task_id='end_dag',
        timeout=10800,
        execution_date_fn=cu.get_execution_date,
        params={"schedule":"15 5 * * *","frequency":"daily","cycle_age":"current"},
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule"
    )
    with TaskGroup(group_id='write_inventory_files') as write_inventory_files:
        for ssc in inventory_dict:
            ssc_name = ssc
            ssc_name_nospace = ssc.replace(" ", "")
            # Check if ssc_name ends with "SSC", and remove the last 3 characters if true
            if ssc_name_nospace.endswith("SSC"):
                group_id_name = ssc_name_nospace[:-3]  # Remove the last 3 characters
            else:
                group_id_name = ssc_name_nospace  # Keep it as is if it doesn't end with "SSC"
            # Print the value of group_id_name to debug
            print(f"group_id_name for {ssc_name}: {group_id_name}")
            
            with TaskGroup(group_id=f'{group_id_name}_inventory_files') as ssc_inventory_file_group:
                inventory_file = BigQueryOperator(
                    task_id='inventory_file_extract',
                    gcp_conn_id=config['env']['v_curated_conn_id'],
                    sql='dml/outbound/inventory_sql.sql',
                    params={"ssc_name_orig": group_id_name ,"ssc_name": ssc_name, "file_prefix": 'chunk_inventory_'},
                    use_legacy_sql=False,
                    retries=3
                )
                inventory_file_consolidation = PythonOperator(
                    task_id='inventory_file_consolidations',
                    python_callable=cu.combine_chunked_files,
                    op_kwargs = {"bucket_name": default_args['params']['param_parallon_ra_data_bucket_name'], "ssc_name": group_id_name, "outbound_file_path": default_args['params']['param_parallon_ra_outbound_file_path'], "extract_file_type":'inventory', "bq_suffix": bq_suffix}
                )
                inventory_audit_file = BigQueryOperator(
                    task_id='inventory_audit_file_extract',
                    gcp_conn_id=config['env']['v_curated_conn_id'],
                    sql='dml/outbound/inventory_sql_audit.sql',
                    params={"ssc_name_orig": group_id_name , "ssc_name": ssc_name, "file_prefix": 'chunk_inventory_audit_'},
                    use_legacy_sql=False,
                    retries=3
                )
                inventory_audit_file_rename = PythonOperator(
                    task_id='inventory_audit_file_rename',
                    python_callable=cu.rename_blob,
                    op_kwargs = {"bucket_name": default_args['params']['param_parallon_ra_data_bucket_name'], "path_to_blob": default_args['params']['param_parallon_ra_outbound_file_path'], "blob_name": f"chunk_inventory_audit_{group_id_name}_000000000000.txt", "new_blob_name": f"{group_id_name}Inventory_Audit{bq_suffix.upper()}.txt"}
                )
                inventory_file >> inventory_file_consolidation >> inventory_audit_file >> inventory_audit_file_rename
            [ssc_inventory_file_group]
    with TaskGroup(group_id='write_denials_files') as write_denials_files:
        for ssc in denials_dict:
            ssc_name = ssc
            ssc_name_nospace = ssc.replace(" ", "")
            # Check if ssc_name ends with "SSC", and remove the last 3 characters if true
            if ssc_name_nospace.endswith("SSC"):
                group_id_name = ssc_name_nospace[:-3]  # Remove the last 3 characters
            else:
                group_id_name = ssc_name_nospace  # Keep it as is if it doesn't end with "SSC"
        
            # Print the value of group_id_name to debug
            print(f"group_id_name for {ssc_name}: {group_id_name}")
            with TaskGroup(group_id=f'{group_id_name}_denials_files') as ssc_denial_file_group:
                denials_file = BigQueryOperator(
                    task_id='denials_file_extract',
                    gcp_conn_id=config['env']['v_curated_conn_id'],
                    sql='dml/outbound/denials_sql.sql',
                    params={"ssc_name_orig": group_id_name , "ssc_name": ssc_name, "file_prefix": 'chunk_denials_'},
                    use_legacy_sql=False,
                    retries=3
                )
                denials_file_consolidation = PythonOperator(
                    task_id='denials_file_consolidations',
                    python_callable=cu.combine_chunked_files,
                    op_kwargs = {"bucket_name": default_args['params']['param_parallon_ra_data_bucket_name'], "ssc_name": group_id_name, "outbound_file_path": default_args['params']['param_parallon_ra_outbound_file_path'], "extract_file_type":'denials', "bq_suffix": bq_suffix}
                )
                denials_audit_file = BigQueryOperator(
                    task_id='denials_audit_file_extract',
                    gcp_conn_id=config['env']['v_curated_conn_id'],
                    sql='dml/outbound/denials_sql_audit.sql',
                    params={"ssc_name_orig": group_id_name, "ssc_name": ssc_name, "file_prefix": 'chunk_denials_audit_'},
                    use_legacy_sql=False,
                    retries=3
                )
                denials_audit_file_rename = PythonOperator(
                    task_id='denials_audit_file_rename',
                    python_callable=cu.rename_blob,
                    op_kwargs = {"bucket_name": default_args['params']['param_parallon_ra_data_bucket_name'], "path_to_blob": default_args['params']['param_parallon_ra_outbound_file_path'], "blob_name": f"chunk_denials_audit_{group_id_name}_000000000000.txt", "new_blob_name": f"{group_id_name}Denials_Audit{bq_suffix.upper()}.txt"}
                )
                denials_file >> denials_file_consolidation >> denials_audit_file >> denials_audit_file_rename
            [ssc_denial_file_group]
    with TaskGroup(group_id='write_custom_files') as write_custom_files:
        for custom_file in custom_conditions_arr:
            ssc_name = custom_file['ssc_name']
            filter_column = custom_file['filter_column']
            filter_value_list= custom_file['filter_value_list']
            with TaskGroup(group_id=f'{ssc_name}_custom_files') as custom_denial_file_group:
                denials_file = BigQueryOperator(
                    task_id='custom_denials_file_extract',
                    gcp_conn_id=config['env']['v_curated_conn_id'],
                    sql='dml/outbound/denials_sql_custom.sql',
                    params={"ssc_name": ssc_name, "filter_column": filter_column, "filter_value_list": filter_value_list, "file_prefix": 'chunk_denials_'},
                    use_legacy_sql=False,
                    retries=3
                )
                denials_file_consolidation = PythonOperator(
                    task_id='denials_file_consolidations',
                    python_callable=cu.combine_chunked_files,
                    op_kwargs = {"bucket_name": default_args['params']['param_parallon_ra_data_bucket_name'], "ssc_name": ssc_name, "outbound_file_path": default_args['params']['param_parallon_ra_outbound_file_path'], "extract_file_type":'denials', "bq_suffix": bq_suffix}
                )
                inventory_file = BigQueryOperator(
                    task_id='custom_inventory_file_extract',
                    gcp_conn_id=config['env']['v_curated_conn_id'],
                    sql='dml/outbound/inventory_sql_custom.sql',
                    params={"ssc_name": ssc_name, "filter_column": filter_column, "filter_value_list": filter_value_list, "file_prefix": 'chunk_inventory_'},
                    use_legacy_sql=False,
                    retries=3
                )
                inventory_file_consolidation = PythonOperator(
                    task_id='inventory_file_consolidations',
                    python_callable=cu.combine_chunked_files,
                    op_kwargs = {"bucket_name": default_args['params']['param_parallon_ra_data_bucket_name'], "ssc_name": ssc_name, "outbound_file_path": default_args['params']['param_parallon_ra_outbound_file_path'], "extract_file_type":'inventory', "bq_suffix": bq_suffix}
                )
                denials_file >> denials_file_consolidation >> inventory_file >> inventory_file_consolidation
            [custom_denial_file_group]

    source_files = []
    dest_files = []
    for ssc in denials_dict:
        ssc_name = ssc.replace(" ", "")          
        if ssc_name.endswith("SSC"):            
            group_id_name = ssc_name[:-3]       
        else:
            group_id_name = ssc_name
        source_files.append(group_id_name + f'Denials{bq_suffix.upper()}.txt')
        source_files.append(group_id_name + f'Denials_Audit{bq_suffix.upper()}.txt')
        dest_files.append(group_id_name + f'Denials{bq_suffix.upper()}.txt')
        dest_files.append(group_id_name + f'Denials_Audit{bq_suffix.upper()}.txt')

    for ssc in inventory_dict:
        ssc_name = ssc.replace(" ", "")          
        if ssc_name.endswith("SSC"):             
            group_id_name = ssc_name[:-3]       
        else:
            group_id_name = ssc_name
        source_files.append(group_id_name + f'Inventory{bq_suffix.upper()}.txt')
        source_files.append(group_id_name + f'Inventory_Audit{bq_suffix.upper()}.txt')
        dest_files.append(group_id_name + f'Inventory{bq_suffix.upper()}.txt')
        dest_files.append(group_id_name + f'Inventory_Audit{bq_suffix.upper()}.txt')

    for custom_file in custom_conditions_arr:
        source_files.append(custom_file['ssc_name'] + f'Denials{bq_suffix.upper()}.txt')
        dest_files.append(custom_file['ssc_name'] + f'Denials{bq_suffix.upper()}.txt')
        source_files.append(custom_file['ssc_name'] + f'Inventory{bq_suffix.upper()}.txt')
        dest_files.append(custom_file['ssc_name'] + f'Inventory{bq_suffix.upper()}.txt')

    source_files_str = ' '.join(source_files)
    dest_files_str = ' '.join(dest_files)
    
    # read_config_file_path = PythonOperator(task_id='read_config_file_path', python_callable=get_config_file_path)                        
    file_export_df_job = BashOperator(
        task_id="file_export_to_server",
        dag = dag,
        bash_command="python /home/airflow/gcs/dags/edwra/scripts/file_export_df.py --source_files {} --dest_files {} --src_sys_config_file={}".format(source_files_str, dest_files_str, config_name)
    )
    start_job >> check_task_completion >> [write_denials_files, write_inventory_files, write_custom_files] >> file_export_df_job >> end_job
