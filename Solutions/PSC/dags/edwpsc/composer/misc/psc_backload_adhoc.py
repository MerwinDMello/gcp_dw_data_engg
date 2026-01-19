from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.exceptions import AirflowSkipException
from datetime import timedelta
import os
import pendulum
import logging
import sys
import json
import re

# ----------------------------
# Custom utilities
# ----------------------------
script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, '../..', 'utils')
sys.path.append(util_dir)
import common_utilities as cu

# GCP Secret Manager (for SQL creds)
from google.cloud import secretmanager

# ----------------------------
# Config
# ----------------------------
lob = "edwpsc"
lob_abbr = lob.replace("edw", "")
src_system = "psc_backload"

config = cu.call_config_yaml(f"{lob_abbr}_config.yaml", f"hca_{lob_abbr}_default_vars")
srcsys_config = cu.call_config_yaml(f"{src_system}_ingest.yaml", f"{src_system}_ingest")
servers = srcsys_config.get("servers", {})

server_map = {
    "server1": config['env']['v_server01'],
    "server2": config['env']['v_server02'],
    "server3": config['env']['v_server03']
}

current_timezone = pendulum.timezone("US/Central")
bq_project_id = config['env']['v_curated_project_id']

default_args = {
    'owner': f'hca_{lob_abbr}_atos',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=480)
}

# ----------------------------
# Helper functions
# ----------------------------
def slug(s: str) -> str:
    """Convert string to lowercase hyphenated format."""
    return s.lower().replace("_", "-").replace('.', '-').replace('[', '').replace(']', '')

def resolve_driver_jars_from_env(env: dict, srcsys_cfg: dict) -> str:
    """Build driverJars path from env.v_dfjarbucket and srcsys_config.v_jdbc_jar."""
    bucket_prefix = env['v_dfjarbucket'].rstrip('/')
    jar_name = srcsys_cfg['v_jdbc_jar']
    return f"{bucket_prefix}/{jar_name}"

def build_full_secret_path(env: dict, srcsys_cfg: dict) -> str:
    """Build full secret path for Secret Manager."""
    return f"{env['v_pwd_secrets_url'].rstrip('/')}/{srcsys_cfg['v_secret_name'].lstrip('/')}"

def fetch_sql_creds_from_secret(secret_url: str) -> dict:
    """Fetch SQL credentials from Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    resp = client.access_secret_version(name=secret_url)
    payload = resp.payload.data.decode("utf-8")
    data = json.loads(payload)
    return {"username": data.get("user"), "password": data.get("password")}

def parse_table_config(table_config_list: list) -> dict:
    """
    Parse table configuration from YAML format:
    Format: priority~source_table~target_table~query~fetch_size
    Example: 1~SSP_Claim_DW.[GCP].[vw_Table]~v_psc_stage_dataset_name.Table~SELECT ...~100000
    """
    if not table_config_list or len(table_config_list) == 0:
        return None
    
    config_str = table_config_list[0] if isinstance(table_config_list, list) else table_config_list
    
    # Split by ~ delimiter
    parts = config_str.split('~', 4)
    if len(parts) < 5:
        logging.warning(f"Invalid table config format: {config_str}")
        return None
    
    priority, source_table, target_table, query, fetch_size = parts
    
    # Clean up query
    query = re.sub(r'\s+', ' ', query.strip())
    
    return {
        "priority": priority.strip(),
        "source_table": source_table.strip(),
        "target_table": target_table.strip(),
        "query": query,
        "fetch_size": fetch_size.strip()
    }

def resolve_target_table(target_table_str: str, env: dict) -> tuple:
    """
    Resolve target table from token or direct reference.
    Examples:
      'v_psc_stage_dataset_name.TableName' -> (resolved_dataset, 'TableName')
      'dataset_name.TableName' -> ('dataset_name', 'TableName')
    Returns (dataset, table_name)
    """
    if '.' in target_table_str:
        left, right = target_table_str.split('.', 1)
        # Check if left side is an env token
        if left in env:
            return (env[left], right)
        elif left.startswith('v_') and left in env:
            return (env[left], right)
        else:
            # Assume it's already dataset.table
            return (left, right)
    
    # Default dataset if no . found
    default_dataset = env.get(f'v_{lob_abbr}_stage_dataset_name', 'default_dataset')
    return (default_dataset, target_table_str)

def extract_db_from_table(source_table: str) -> str:
    """
    Extract database name from source table name.
    """
    s = source_table.strip()
    if not s or s.startswith('['):
        return None
    if '.' in s:
        return s.split('.', 1)[0]
    return None

# ----------------------------
# DAG definition
# ----------------------------
with DAG(
    dag_id="psc_backload_pipeline_multi_server_adhoc",
    default_args=default_args,
    schedule=None,
    catchup=False,
    params={
        "run_config": Param(
            default={
                "tables": [
                    {
                        "name": "vw_Artiva_FactAddress",
                        "where_clause": ""
                    }
                ]
            },
            type="object",
            description="Tables to process with optional where_clause override"
        )
    },
    start_date=pendulum.datetime(2025, 1, 1, tz=current_timezone)
) as dag:

    # ----------------------------
    # Fetch and parse run_config at runtime
    # ----------------------------
    def get_run_list(**context):
        dag_run_conf = context['dag_run'].conf or {}
        run_config = dag_run_conf.get("run_config", {})
        
        # Parse table configurations
        table_configs = {}
        for tbl in run_config.get("tables", []):
            if isinstance(tbl, dict):
                table_name = tbl.get("name", "")
                if table_name:
                    normalized_name = table_name.lower().replace('.', '_').replace('[','').replace(']','')
                    table_configs[normalized_name] = {
                        "name": table_name,
                        "where_clause": tbl.get("where_clause", "")
                    }
        
        logging.info(f"Parsed run_config with {len(table_configs)} tables: {list(table_configs.keys())}")
        return table_configs

    get_run_list_task = PythonOperator(
        task_id='get_run_list',
        python_callable=get_run_list,
        provide_context=True
    )

    # ----------------------------
    # Get SQL credentials from Secret Manager
    # ----------------------------
    full_secret_path = build_full_secret_path(config['env'], srcsys_config)

    get_sql_creds = PythonOperator(
        task_id="get_sql_creds",
        python_callable=lambda: fetch_sql_creds_from_secret(full_secret_path),
    )

    # ----------------------------
    # Validation and config building function
    # ----------------------------
    def validate_and_build_config(table_name, server_name, table_yaml_config, **context):
        """Validate if table should run and build its complete configuration."""
        table_configs = context['task_instance'].xcom_pull(task_ids='get_run_list') or {}
        safe_table = table_name.lower().replace('.', '_').replace('[','').replace(']','')
        
        # Check if table is in run_config
        if safe_table not in table_configs:
            logging.info(f"Skipping {table_name} on {server_name} - not in run_config")
            raise AirflowSkipException(f"Table {table_name} not in run list")
        
        # Parse YAML configuration
        parsed_config = parse_table_config(table_yaml_config)
        if not parsed_config:
            raise AirflowSkipException(f"Invalid table configuration for {table_name}")
        
        # Get runtime where_clause override if provided
        runtime_config = table_configs[safe_table]
        where_clause_override = runtime_config.get("where_clause", "")
        
        # Apply where_clause override if provided
        query = parsed_config["query"]
        if where_clause_override:
            # If query already has WHERE clause, append with AND
            if "WHERE" in query.upper():
                query = f"{query} AND {where_clause_override}"
            else:
                query = f"{query} WHERE {where_clause_override}"
        
        logging.info(f"Processing {table_name} on {server_name}")
        logging.info(f"  - Source Table: {parsed_config['source_table']}")
        logging.info(f"  - Target Table: {parsed_config['target_table']}")
        logging.info(f"  - Fetch Size: {parsed_config['fetch_size']}")
        logging.info(f"  - Where Clause Override: '{where_clause_override}'")
        logging.info(f"  - Final Query: {query[:200]}...")
        
        return {
            "source_table": parsed_config["source_table"],
            "target_table": parsed_config["target_table"],
            "query": query,
            "fetch_size": parsed_config["fetch_size"],
            "priority": parsed_config["priority"]
        }

    # ----------------------------
    # Build table organization by server and database
    # ----------------------------
    server_db_tables = {}
    
    for server_name, srv_data in servers.items():
        if server_name not in server_db_tables:
            server_db_tables[server_name] = {}
        
        for table_name, tbl_config in srv_data.get("tables", {}).items():
            # Parse config to extract database name
            parsed = parse_table_config(tbl_config)
            if not parsed:
                continue
            
            # Extract database name from source table
            db_name = extract_db_from_table(parsed["source_table"])
            if not db_name:
                db_name = 'default'
            
            if db_name not in server_db_tables[server_name]:
                server_db_tables[server_name][db_name] = []
            
            server_db_tables[server_name][db_name].append({
                "table_name": table_name,
                "table_config": tbl_config
            })

    # ----------------------------
    # Flex Template configuration
    # ----------------------------
    template_spec = f"gs://dataflow-templates-{config['env']['v_region']}/latest/flex/Jdbc_to_BigQuery_Flex"
    driver_jars_gcs = resolve_driver_jars_from_env(config['env'], srcsys_config)

    # ----------------------------
    # Create nested TaskGroups: Server -> Database -> Tables
    # ----------------------------
    for server_name in sorted(server_db_tables.keys()):
        safe_server_name = server_name.replace('.', '_')
        actual_server = server_map.get(server_name, server_name)
        
        # Server-level TaskGroup
        with TaskGroup(group_id=f"Server_{safe_server_name}") as server_tg:
            
            for db_name in sorted(server_db_tables[server_name].keys()):
                safe_db_name = db_name.replace('.', '_').replace('[','').replace(']','')
                
                # Database-level TaskGroup
                with TaskGroup(group_id=f"DB_{safe_db_name}") as db_tg:
                    
                    for table_info in server_db_tables[server_name][db_name]:
                        table_name = table_info["table_name"]
                        table_yaml_config = table_info["table_config"]
                        safe_table_name = table_name.replace('.', '_').replace('[','').replace(']','')
                        
                        # Validation and config building task
                        validate_task = PythonOperator(
                            task_id=f"{safe_table_name}_validate",
                            python_callable=validate_and_build_config,
                            op_kwargs={
                                "table_name": table_name,
                                "server_name": server_name,
                                "table_yaml_config": table_yaml_config
                            },
                            provide_context=True
                        )
                        
                        # Parse config to get source table and target info
                        parsed = parse_table_config(table_yaml_config)
                        source_table = parsed["source_table"]
                        target_table_str = parsed["target_table"]
                        
                        # Extract DB name and build connection URL
                        db_name_from_table = extract_db_from_table(source_table)
                        base_url = f"jdbc:sqlserver://{actual_server}:{srcsys_config.get('v_port', 1433)};"
                        conn_props = []
                        if db_name_from_table:
                            conn_props.append(f"databaseName={db_name_from_table}")
                        conn_props.append("encrypt=true")
                        conn_props.append("trustServerCertificate=true")
                        connection_url = base_url + ";".join(conn_props)
                        
                        # Resolve target dataset and table
                        target_dataset, target_table = resolve_target_table(target_table_str, config['env'])
                        output_table = f"{bq_project_id}:{target_dataset}.{target_table}"
                        
                        # Build job name prefix
                        job_name_prefix = f"jdbc-to-bq-{slug(src_system)}-{slug(safe_table_name)}"
                        
                        # Run Dataflow Flex Template
                        run_flex_template = DataflowStartFlexTemplateOperator(
                            task_id=f"{safe_table_name}_run",
                            project_id=config['env']['v_proc_project_id'],
                            location=config['env']['v_region'],
                            body={
                                "launch_parameter": {
                                    "jobName": f"{job_name_prefix}-{{{{ ds_nodash }}}}",
                                    "containerSpecGcsPath": template_spec,
                                    "parameters": {
                                        # JDBC Configuration
                                        "driverJars": driver_jars_gcs,
                                        "driverClassName": srcsys_config['v_jdbc_class_name'],
                                        "connectionURL": connection_url,
                                        
                                        # Credentials from Secret Manager
                                        "username": "{{ ti.xcom_pull(task_ids='get_sql_creds')['username'] }}",
                                        "password": "{{ ti.xcom_pull(task_ids='get_sql_creds')['password'] }}",
                                        
                                        # BigQuery Output
                                        "outputTable": output_table,
                                        "bigQueryLoadingTemporaryDirectory": config['env']['v_gcs_temp_bucket'],
                                        
                                        # Load behavior - always truncate for these tables
                                        "isTruncate": "true",
                                        
                                        # Query from YAML (with potential where_clause override)
                                        "query": "{{ ti.xcom_pull(task_ids='Server_" + safe_server_name + ".DB_" + safe_db_name + "." + safe_table_name + "_validate')['query'] }}",
                                        
                                        # Fetch size from YAML
                                        "fetchSize": "{{ ti.xcom_pull(task_ids='Server_" + safe_server_name + ".DB_" + safe_db_name + "." + safe_table_name + "_validate')['fetch_size'] }}",
                                    },
                                    "environment": {
                                        "serviceAccountEmail": config['env']['v_serviceaccountemail'],
                                        "tempLocation": config['env']['v_gcs_temp_bucket'],
                                        "network": config['env'].get('v_network'),
                                        "subnetwork": config['env'].get('v_subnetwork'),
                                        "ipConfiguration": "WORKER_IP_PRIVATE",
                                        "numWorkers": int(config['env'].get('v_numworkers', 1)),
                                        "maxWorkers": int(config['env'].get('v_maxworkers', 10)),
                                        "machineType": srcsys_config.get('v_machine_type', config['env'].get('v_machinetype', 'n2-standard-8')),
                                    },
                                }
                            },
                            wait_until_finished=True,
                            drain_pipeline=False,
                            do_xcom_push=True,
                        )
                        
                        # Task dependencies
                        validate_task >> run_flex_template
            
            # Connect get_sql_creds to server task group
            get_sql_creds >> server_tg
    
    # Connect get_run_list to get_sql_creds
    get_run_list_task >> get_sql_creds