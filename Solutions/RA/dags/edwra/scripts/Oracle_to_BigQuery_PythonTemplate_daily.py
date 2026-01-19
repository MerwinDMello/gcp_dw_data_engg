import logging
import os
import decimal
import traceback
from google.cloud import bigquery
import apache_beam as beam
import sys
import pandas as pd
from pandas.errors import OutOfBoundsDatetime
import yaml
import json
import concurrent.futures
from google.cloud import secretmanager
import argparse
import string
import random
from datetime import datetime as dt
import time
import pendulum
import math
import numpy as np

timezone_str = "US/Central"
timezone = pendulum.timezone(timezone_str)

cwd = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.dirname(cwd)
sys.path.insert(0, base_dir)
config_folder = base_dir + "/config/"
utilpath = os.path.join(base_dir, "utils")
sys.path.append(utilpath)

# session_parallelization_tables = ['mon_account_payer_calc_service', 'mon_account_payer_calc_coin_fs', 'apg_calc_output', 'mon_account_payer_calc_apc', 'mon_account_payer_calc_fs']
session_parallelization_tables = [ 'mon_account_payer_calc_coin_fs', 'apg_calc_output', 'mon_account_payer_calc_apc', 'mon_account_payer_calc_fs']



def call_config_yaml(filename, variablename):
    # load input config .yaml file from config folder
    cfgfile = open(config_folder + filename)
    default_config = yaml.safe_load(cfgfile)
    default_config = json.loads(json.dumps(default_config, default=str))
    config = default_config
    return config


config = call_config_yaml("ra_config.yaml","hca_ra_default_vars")
 # To allow for concurrent runs of new BQ process and old process, certain tables/files have _BQ suffix
bq_suffix =  config['env']['v_bq_suffix']

def access_secret(secret_resourceid: str) -> str:
    '''Retrieve and decode password from GCP Secret Manager based on secret name'''
    client = secretmanager.SecretManagerServiceClient()
    payload = client.access_secret_version(name=secret_resourceid).payload.data.decode("UTF-8")
    return payload

# List of datetime formats that you want to handle
datetime_formats = [
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f"
    # Add any other datetime formats that your data might have
]

def to_str(x):
    if pd.isnull(x):
        return None
    else:
        return str(x)

def to_int(x):
    try:
        return int(x)
    except:
        return np.nan

def to_decimal(value):
    try:
        return decimal.Decimal(value)
    except:
        return np.nan

# Converting to float will cause data loss for digits > 17
def to_bignumeric(value):
    if pd.isnull(value):
        return np.nan
    else:
        return str(value)

def convert_datetime(x):
    if pd.isnull(x):
        return np.nan
    
    if x == None:
        return np.nan
    
    for fmt in datetime_formats:
        try:
            # If the datetime string can be converted using the current format, do the conversion
            dte = dt.strptime(str(x), fmt)
            # Then convert it to the desired format and return
            return dt(dte.year, dte.month, dte.day, dte.hour, dte.minute, dte.second)
        except Exception as e:
            # logging.info('Failed datetime conversion')
            continue
    
    # Print an error message if none of the formats match (you could also raise an error or handle this differently depending on your needs)
    print(f"Error: Could not convert '{x}' to datetime")
    return np.nan



class setuprunnerenv(beam.DoFn):
    def process(self, context):
        # use /tmp/ on dataflow worker node for processing
        global base_dir
        base_dir = '/tmp/'

        os.system('java -version')

        return list("1")


class jdbctobq(beam.DoFn):

    def executevalidationsqls(self, bq_table):
        bq_table = bq_table.lower()

        if config['env']['v_runner'] == 'DataflowRunner':
            sqlbucket = 'gs://' + \
                        config['env']['v_dag_bucket_name'] + \
                        '/dags/edwra/sql/validation_sql/'
            os.system('gsutil cp ' + sqlbucket + '*' + bq_table +
                      '.sql' + ' ' + base_dir + ' && ls ')
            logging.info(
                "===Copying Validation SQL's for table {} if any to dataflow runner===".format(bq_table))
            os.system('gsutil cp ' + sqlbucket + '*' + bq_table +
                      '.sql' + ' ' + base_dir + ' && ls ')
        else:
            logging.info(
                "===Checking for Validation SQL's for table {} if any locally===".format(bq_table))

        file_list = [a for a in os.listdir(
            base_dir) if a.endswith(bq_table + ".sql")]

        if file_list:
            for filename in file_list:
                bqsqlsqryfile = open(base_dir + '\\' + filename)
                bqsqlsqry = bqsqlsqryfile.read()
                logging.info("===Executing SQL : {}===".format(filename))
                df = pd.read_gbq(bqsqlsqry, project_id=bqproject_id)
                logging.info(df)
        else:
            logging.info(
                "===Did not find any Validation SQL for table - {}===".format(bq_table))

    def readjdbcwritebqtable(self, tableinfo, bqproject_id):
        import jaydebeapi
        import oracledb
        import pendulum
        import pandas_gbq
        import io
        import re
        from jaydebeapi import _DEFAULT_CONVERTERS
        from datetime import datetime
        import decimal
        decimal_context = decimal.Context(prec=13)
        bq_project_id = config['env']['v_curated_project_id']
        env = config['env']['v_env_name']
        bqclient = bigquery.Client(bq_project_id)
        if src_schema_id == '1':
            passwd = access_secret(config['env']['v_pwd_secrets_url'] + ra_oracle['v_pwd_secret_name_p1'])
            service_name = ra_oracle['servicename_p1']
        elif src_schema_id == '3':
            passwd = access_secret(config['env']['v_pwd_secrets_url'] + ra_oracle['v_pwd_secret_name_p2'])
            service_name = ra_oracle['servicename_p2']    
        user = ra_oracle['v_user']
        host_name = ra_oracle['hostname']

        logging.info(f'Using service {service_name}')
        logging.info(f"Using secret with name {config['env']['v_pwd_secrets_url']}")

        # Override jaydebeapi package default converter to fix milliseconds processing bug
        def _to_datetime(rs, col):

            java_val = rs.getTimestamp(col)
            if not java_val:
                return
            d = datetime.strptime(str(java_val)[:19], "%Y-%m-%d %H:%M:%S")
            d = d.replace(microsecond=java_val.getNanos() // 1000)
            return str(d)

        _DEFAULT_CONVERTERS.update({"TIMESTAMP": _to_datetime})

        def get_edw_etl_load_bq_field(col_expression: str, table_name: str) -> str:
            '''Query BQ table to get column value to insert into query'''
            query_str = "SELECT {} as val FROM {}.edw_etl_load WHERE Schema_Id = {} and lower(table_name) = LOWER('{}');".format(col_expression, bq_stage_dataset, src_schema_id, table_name)
            logging.info("===Running query===")
            logging.info(str(query_str))
            res_df = pd.read_gbq(query_str, project_id=bq_project_id)
            logging.info("===Done running query===")
            bq_field = res_df['val'][0]
            return bq_field

        def replace_edw_etl_load_field(sql: str, table_name: str) -> str:
            '''Replaces references to HCA_REPORTING.EDW_ETL_LOAD table with the output of that query from the BQ table'''
            logging.info("Replacing edw_etl_load fields")
            table_name = 'mon_account_nonclinical_code_c' if table_name == 'mon_account_nonclinical_code_del' else table_name
            # Regex pattern to find subqueries referencing EDW_ETL_LOAD table
            pattern = r"\(\s*SELECT\s+([^()]*?)\s+FROM\s+[^()]*?\s+WHERE[^()]*?TABLE_NAME\s*=\s*'([^']*)'.*?\)"
            def replace_re(match):
                col_expression = match.group(1)
                # Sometimes table has alias, remove from field name select if that is the case
                col_expression = col_expression.split('.')[-1]
                # Sometimes value for table_name filter is a env variable ($) instead of string, handle both cases
                table_name_to_query = table_name if '$' in match.group(2) else match.group(2)
                bq_val = get_edw_etl_load_bq_field(col_expression, table_name_to_query)
                oracle_val_string = "TO_DATE('{}', 'YYYY-MM-DD HH24:MI:SS')".format(bq_val)
                return oracle_val_string
            # Replace all occurrences of that pattern with the corresponding value from the BQ EDW_ETL_LOAD table
            replaced_sql = re.sub(pattern, replace_re, sql, flags=re.IGNORECASE)
            return replaced_sql
        
        def fetch_data_from_oracle(partition, query):
            attempt_limit = 3
            for attempt in range(attempt_limit):
                try:
                    conn = conn_pool.acquire()
                    logging.info(f"Successfully connected to Oracle for partition {partition}.")
                    if is_partitioned:
                        formatted_clause = f"AND MOD(ABS({partition_column}),{num_partitions}) = {partition}"
                        modified_query = f"{query} {formatted_clause}"
                    else:
                        modified_query = query
                    total_rows = 0
                    # To resolve slowness for certain tables in D5/D6. Can be removed when no longer an issue
                    if table_to_load in session_parallelization_tables and env in ['dev','qa']:
                        logging.info("Running alt process to alter session to run query in parallel")
                        chunk_counter = 0
                        with conn.cursor() as cursor:
                            cursor.execute("ALTER SESSION FORCE PARALLEL QUERY PARALLEL 16")
                            cursor.execute(modified_query)
                            logging.info(f"Succesfully executed query for partition {partition}")
                            while True:
                                logging.info("Fetching another chunk")
                                chunk = cursor.fetchmany(chunksize)
                                if not chunk:
                                    break
                                header = [i[0] for i in cursor.description]
                                df_chunk = pd.DataFrame(chunk, columns=header)
                                logging.info(f'Generated chunk {chunk_counter} for partition {partition}')
                                chunk_counter += 1
                                total_rows += len(df_chunk)
                                yield df_chunk
                    else:
                        chunks = pd.read_sql(modified_query, conn, chunksize=chunksize)
                        for chunk_index, df_chunk in enumerate(chunks):
                            logging.info(f'Generated chunk {chunk_index} for partition {partition}')
                            total_rows += len(df_chunk)
                            yield df_chunk
                    logging.info(f"Total rows processed for partition {partition}: {total_rows}")
                    conn.close()
                    break  # Break the loop if successful
                except Exception as e:
                    logging.error(f"ODBC Error occurred for partition {partition}: %s", e)
                    if 'DPY-4011' in str(e) and attempt < attempt_limit - 1:
                        if is_partitioned:
                            partition_column_bq = partition_column.split('.')[-1]
                            formatted_clause = f"AND MOD(ABS({partition_column_bq}),{num_partitions}) = {partition}"
                            delete_from_temp_bigquery_table(formatted_clause)
                        else:
                            delete_from_temp_bigquery_table()
                        logging.info(f"Attempting to reconnect due to connection closed for partion {partition}...")
                        time.sleep(2 ** attempt) + random.random()  # Exponential backoff
                    else:
                        raise
                # finally:
                #     try:
                #         conn.close()
                #         logging.info(f"Database connection closed for partition {partition}.")
                #     except:
                #         logging.warning(f"Failed to close database connection for partition {partition}")
                
        def delete_from_temp_bigquery_table(partition_clause=None):
            delete_query = f"DELETE FROM {temp_table_to_load_full} WHERE schema_id = {src_schema_id}"
            if partition_clause is not None:
                delete_query = f"DELETE FROM {temp_table_to_load_full} WHERE schema_id = {src_schema_id} {partition_clause}"
            bqclient.query(delete_query).result()
            logging.info(f"===Deleted from temp table using query: {delete_query} ===")

        def write_data_to_bigquery(data, partition):
            job_config = bigquery.LoadJobConfig()
            job_config.autodetect = False
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            job_config.schema = tblschema

            for attempt in range(5):  # Retry up to 5 times
                try:
                    logging.info(f'Attempting to load {len(data)} rows to BigQuery for partition {partition}.')
                    pandas_gbq.to_gbq(data, temp_table_to_load_full,project_id=bq_project_id, if_exists='append', table_schema=tblschema)
                    logging.info(f"{len(data)} rows loaded successfully to BigQuery for partition {partition}.")
                    return len(data)
                except Exception as e:
                    print(str(e))
                    if 'Exceeded rate limits' in str(e) or "Retrying" in str(e):
                        # Exponential backoff formula
                        sleep_time = (2 ** attempt) + random.random()
                        logging.warning(
                            f"Rate limit exceeded. Retrying in {sleep_time:.2f} seconds...")
                        time.sleep(sleep_time)
                    else:
                        logging.error("Failed to load data to BigQuery: %s", e)
                        raise  # Reraise the exception if it's not a rate limit issue
            raise Exception("Failed to load data after multiple retries")

        def process_chunk(chunk: pd.DataFrame, partition):
            chunk.columns = map(str.lower, chunk.columns)
            col_spl_char = ['.', '[', ']']
            for char in col_spl_char:
                chunk.columns = chunk.columns.str.replace(char, '_', regex=False)

            for x in table.schema:
                dtype = x.field_type.lower()
                if dtype == 'time':
                    dtype = 'datetime64[ns]'
                elif dtype == 'integer':
                    dtype = 'Int64'
                elif dtype == 'numeric':
                    dtype = 'float64'
                elif dtype == 'string':
                    dtype = 'str'

                # Converting float column to decimal to match numeric in BQ
                if dtype == 'float64':
                    chunk[x.name] = chunk[x.name].apply(dtype)
                    chunk[x.name] = chunk[x.name].apply(decimal_context.create_decimal_from_float)
                
                elif dtype == 'bignumeric':
                    chunk[x.name] = chunk[x.name].apply(to_bignumeric)
                
                elif dtype == 'datetime' or dtype == 'date':                                                       
                    try:
                        chunk[x.name] = pd.to_datetime(chunk[x.name])
                    except OutOfBoundsDatetime:
                        # Out of Bounds Exception. Trying different conversion
                        chunk[x.name] = chunk[x.name].apply(convert_datetime)
                        if chunk[x.name].dtype != 'object':
                            chunk[x.name] = pd.to_datetime(chunk[x.name])

                elif dtype == 'Int64':
                    chunk[x.name] = chunk[x.name].apply(to_int)
                    chunk[x.name] = chunk[x.name].astype(dtype)

                elif dtype == 'str':
                    chunk[x.name] = chunk[x.name].apply(to_str)

                else:
                    chunk[x.name] = np.nan
                    chunk[x.name] = chunk[x.name].astype(dtype)

            chunk.replace(['nan'], None, inplace=True)
            return chunk
        
        def process_partition(partition, query):
            total_rows = 0
            chunk_generator = fetch_data_from_oracle(
                partition,
                query
            )

            for data_chunk in chunk_generator:
                logging.info(f'Beginning to process the chunk for partition {partition}')
                data_chunk = process_chunk(data_chunk, partition)
                logging.info(f'Finished processing the chunk for partition {partition}')
                num_rows_loaded = write_data_to_bigquery(
                    data_chunk, partition)
                total_rows += len(data_chunk)
                logging.info(
                    f"{num_rows_loaded} rows loaded successfully to BigQuery for partition {partition}")
            logging.info(f'Returning {total_rows}')
            return total_rows

        def load_calc_miss_to_oracle(id_table: str, staging_table: str, concuity_table: str):
            ''''''
            logging.info("Beginning MAPCL Miss load to Concuity")
            mapcl_miss_sql = f'''SELECT
                mapcli.id,
                mapcli.schema_id,
                timestamp_trunc(current_timestamp(), SECOND) AS last_refresh_date_time
            FROM
                {bq_stage_dataset}.{id_table} AS mapcli
            WHERE NOT EXISTS (
                SELECT
                    1
                FROM
                    {bq_stage_dataset}.{staging_table} AS mapcl
                WHERE mapcl.schema_id = mapcli.schema_id
                AND mapcl.id = mapcli.id
            )
            AND mapcli.schema_id = {src_schema_id};'''
            bq_df = pd.read_gbq(mapcl_miss_sql, project_id=bq_project_id)
            logging.info(f"Loaded {len(bq_df)} rows from BQ, inserting to Concuity table {concuity_table}")
            bq_df.to_sql(concuity_table, con=conn_str_sa, if_exists='append', schema='HCA_REPORTING', index=False)
            logging.info("Finished MAPCL Miss load to Concuity")
        
        # Function to allow for writeback to Concuity in dev
        # Writes back to same table in same database as qa, so using surrogate schema_id
        def load_calc_miss_to_oracle_dev(id_table: str, staging_table: str, concuity_table: str):
            ''''''
            logging.info("Beginning MAPCL Miss load to Concuity")
            mapcl_miss_sql = f'''SELECT
                mapcli.id as id,
                {str(src_schema_id)*2} as schema_id,
                timestamp_trunc(current_timestamp(), SECOND) AS last_refresh_date_time
            FROM
                {bq_stage_dataset}.{id_table} AS mapcli
            WHERE NOT EXISTS (
                SELECT
                    1
                FROM
                    {bq_stage_dataset}.{staging_table} AS mapcl
                WHERE mapcl.schema_id = mapcli.schema_id
                AND mapcl.id = mapcli.id
            )
            AND mapcli.schema_id = {src_schema_id};'''
            bq_df = pd.read_gbq(mapcl_miss_sql, project_id=bq_project_id)
            logging.info(f"Loaded {len(bq_df)} rows from BQ, inserting to Concuity table {concuity_table}")
            bq_df.to_sql(concuity_table, con=conn_str_sa, if_exists='append', schema='HCA_REPORTING', index=False)
            logging.info("Finished MAPCL Miss load to Concuity")

        def etl_cc_source_rows_count_cleanup():
            cleanup_query = f'''DELETE FROM {bq_stage_dataset}.etl_cc_source_row_counts
            WHERE DATE(dw_last_update_date) < date_add(current_date('{timezone_str}'), interval -13 MONTH);'''
            bqclient.query(cleanup_query).result()
            logging.info(f'Deleted old etl row counts (>13 months old) from {bq_stage_dataset}.etl_cc_source_row_counts')

        def check_table_load_status(table_name, src_schema_id):
            if src_schema_id == '1':
                query = f"SELECT CASE WHEN CAST(MAX(audit_time) AS DATE) = CURRENT_DATE() THEN 1 ELSE 0 END AS is_loaded_today FROM edwra_ac.audit_control WHERE tgt_tbl_nm = '{table_name}' AND audit_status = 'PASS' AND CONTAINS_SUBSTR(job_name, 'p1')"
            elif src_schema_id == '3':
                query = f"SELECT CASE WHEN CAST(MAX(audit_time) AS DATE) = CURRENT_DATE() THEN 1 ELSE 0 END AS is_loaded_today FROM edwra_ac.audit_control WHERE tgt_tbl_nm = '{table_name}' AND audit_status = 'PASS' AND CONTAINS_SUBSTR(job_name, 'p2')"
            res = pd.read_gbq(query, bq_project_id)
            is_loaded_today  = res['is_loaded_today'][0]
            if is_loaded_today == 1:
                return True
            else: 
                return False

        tableload_start_time = str(pendulum.now(timezone))[:23]
        # read input table info and extract table/query details
        tableinfo_list = tableinfo.split("~")
        srctableid = tableinfo_list[0]
        srctablename = tableinfo_list[1]
        tgttablename = tableinfo_list[2]
        tgttablename = tgttablename.replace('v_parallon_ra_stage_dataset_name', bq_stage_dataset)
        tgttablenamecdc = tableinfo_list[3]
        tgttablenamecdc = tgttablenamecdc.replace('v_parallon_ra_stage_dataset_name', bq_stage_dataset)
        is_cdc = False if tgttablenamecdc == '' else True
        table_to_load_full = tgttablenamecdc if is_cdc else tgttablename
        table_to_load = table_to_load_full.split('.')[-1]
        tgttableloadtype = tableinfo_list[5]
        # tables_to_load_directly = ['mon_account_nonclinical_code_del', 'etl_cc_source_row_counts', 'mapcl_id', 'mapcl_svc_id']
        tables_to_load_directly = ['mon_account_nonclinical_code_del', 'etl_cc_source_row_counts', 'mapcl_id']
        if table_to_load in tables_to_load_directly:
            temp_table_to_load_full = table_to_load_full
        else:
            temp_table_to_load_full = table_to_load_full + "_temp"
        primary_keys = tableinfo_list[6]
        if primary_keys != '' or primary_keys != 'N/A':
            primary_keys = [k.strip().lower() for k in primary_keys.split(',')]
        is_calc_job = True if tableinfo_list[7] == "Yes" else False
        srctablequery = tableinfo_list[8]
        srctablequery = srctablequery.replace('v_curr_timestamp', tableload_start_time)
        srctablequery = srctablequery.replace('$BQ_SUFFIX', bq_suffix.upper())
        # Logic to allow for concurrent dev/qa runs of Concuity writeback process
        if env == 'dev':
            srctablequery = srctablequery.replace('$SchemaIDMiss', str(src_schema_id)*2)
        else:
            srctablequery = srctablequery.replace('$SchemaIDMiss', src_schema_id)
        srctablequery = srctablequery.replace('$SchemaID', src_schema_id)
        
        # Replace all whitespace with a single space
        srctablequery = ' '.join(srctablequery.split())

        srctablequeryfull = tableinfo_list[9]
        srctablequeryfull = srctablequeryfull.replace('v_curr_timestamp', tableload_start_time)
        srctablequeryfull = srctablequeryfull.replace('$BQ_SUFFIX', bq_suffix.upper())
        # Logic to allow for concurrent dev/qa runs of Concuity writeback process
        if env == 'dev':
            srctablequeryfull = srctablequeryfull.replace('$SchemaIDMiss', str(src_schema_id)*2)
        else:
            srctablequeryfull = srctablequeryfull.replace('$SchemaIDMiss', src_schema_id)
        srctablequeryfull = srctablequeryfull.replace('$SchemaID', src_schema_id)
        # Replace all whitespace with a single space
        srctablequeryfull = ' '.join(srctablequeryfull.split())
        edw_etl_load_pre_update = True if tableinfo_list[10] == 'Yes' else False
        edw_etl_load_post_update = True if tableinfo_list[11] == 'Yes' else False
        chunksize = int(tableinfo_list[12])
        
        if len(tableinfo_list) > 13:
            num_partitions = int(tableinfo_list[13])
            partition_column = tableinfo_list[14]
            if num_partitions > 1:
                is_partitioned = True
            else:
                is_partitioned = False
        else:
            is_partitioned = False

        # Uncomment below section to check the load status of the table and skip the load if it has already been successfully loaded for the day
        # is_loaded_today = check_table_load_status(table_to_load_full, src_schema_id)
        # if is_loaded_today is True:
        #     logging.info('Table has already been loaded today, continuing to the next table')
        #     return

        logging.info("===Starting process to  extract {} and load {} at {}===".format(
            srctablename, table_to_load, time.strftime("%Y%m%d-%H:%M:%S")))
        job_config = bigquery.job.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        dataset_ref = bqclient.dataset(bq_stage_dataset)
        table_ref = dataset_ref.table(table_to_load)
        table = bqclient.get_table(table_ref)
        f = io.StringIO("")
        bqclient.schema_to_json(table.schema, f)
        tblschema = json.loads(f.getvalue())

        if edw_etl_load_pre_update and table_to_load not in tables_to_ignore:
            logging.info(f"Beginning edw_etl_load table pre processing update for {table_to_load}")
            for attempt in range(1, 8):
                try:
                    update_sql_pre = f'''UPDATE `{bq_stage_dataset}.edw_etl_load`
                    SET CURRENT_EFF_TO_DATE_TIME = DATE_SUB(DATETIME_TRUNC(CURRENT_DATETIME('{timezone_str}'), SECOND), INTERVAL 5 MINUTE),
                    CURRENT_EFF_FROM_DATE_TIME = LAST_EFF_TO_DATE_TIME,
                    LAST_UPDATE_DATE_TIME = DATETIME_TRUNC(CURRENT_DATETIME('{timezone_str}'), SECOND)
                    WHERE SCHEMA_ID = {src_schema_id} AND lower(TABLE_NAME) = lower('{table_to_load}');
                    '''
                    bqclient.query(update_sql_pre).result()
                    logging.info(f"Completed edw_etl_load table pre processing update for {table_to_load}")
                    break
                except Exception as e:
                    logging.info("edw_etl_load update failed due to too many concurrent updates, retrying")
                    wait_time = (math.ceil(2.5 ** attempt))
                    time.sleep(wait_time)

        # In source queries, HCA_REPORTING.EDW_ETL_LOAD table is queried. We are now tracking that in BQ.
        # The following code will replace the subquery that selects from HCA_REPORTING with the result from querying in BQ.
        srctablequery = replace_edw_etl_load_field(srctablequery, table_to_load)
        srctablequeryfull = replace_edw_etl_load_field(srctablequeryfull, table_to_load)
        logging.info("Done replacing EDW ETL fields")
        
        if is_calc_job:
            if 'all_calc_tables' in tables_to_ignore:
                logging.info("This is a calc job and a weekend run so a full pull will be done in a separate Dataflow job. Continuing to the next table")
                return
            else:
                logging.info("This is a calc job but a weekday run so the normal query will be used")
        
        if is_cdc and table_to_load in tables_to_ignore:
            logging.info("This is a cdc job, but a full pull is being performed in a separate Dataflow job. Continuing to the next table")
            return
        
        delete_from_temp_bigquery_table()

        load_count = 0
        # Remove once temporary writeback process to D5/D6 is removed
        # tables_using_miss = ['mon_account_payer_calc_latest', 'mon_account_payer_apc_comp_dtl', 'mon_account_payer_calc_service', 'mon_account_payer_calc_apc', 'mon_account_payer_calc_coin_fs', 'mon_account_payer_calc_fs', 'apg_calc_output', 'mapcl_id', 'mapcl_svc_id']
        #Removed 'mon_account_payer_calc_service', 'mapcl_svc_id'
        tables_using_miss = ['mon_account_payer_calc_latest', 'mon_account_payer_apc_comp_dtl',  'mon_account_payer_calc_apc', 'mon_account_payer_calc_coin_fs', 'mon_account_payer_calc_fs', 'apg_calc_output', 'mapcl_id']
        
        # tables_using_miss = []
        if env in ['dev','qa']:
            if table_to_load in tables_using_miss:
                logging.info('Table uses _MISS tables, connecting to D5/D6 instance for workaround')
                if src_schema_id == '1':
                    # passwd = access_secret(config['env']['v_pwd_secrets_url'] + ra_oracle['v_pwd_secret_name_d5'])
                    passwd = access_secret(config['env']['v_pwd_secrets_url'] + ra_oracle['v_pwd_secret_name_p1'])
                    # service_name = ra_oracle['servicename_d5']
                    service_name = ra_oracle['servicename_p1']
                elif src_schema_id == '3':
                    # passwd = access_secret(config['env']['v_pwd_secrets_url'] + ra_oracle['v_pwd_secret_name_d6'])
                    passwd = access_secret(config['env']['v_pwd_secrets_url'] + ra_oracle['v_pwd_secret_name_p2'])
                    # service_name = ra_oracle['servicename_d6']
                    service_name = ra_oracle['servicename_p2']
                srctablequery = srctablequery.replace('CONCUITY.', 'RPT_EXTRACT.')
            else:
                logging.info(f'Table {table_to_load} not in {str(tables_using_miss)}')
        
        logging.info("===Target load table is {}===".format(temp_table_to_load_full))
        logging.info("===query being used is : {}===".format(srctablequery))
        
        conn_str_sa = f'oracle+oracledb://{user}:{passwd}@{host_name}/?service_name={service_name}'
        # Create connection pool for threads to acquire connections from
        conn_pool = oracledb.create_pool(user=user, password=passwd, dsn=f'{host_name}/{service_name}',min=1, max=32, increment=1)
        count_conn = conn_pool.acquire() 
        count_query="select count(*) as count from ({}) a".format(srctablequery)
        # To resolve slowness for certain tables in D5/D6. Can be removed when no longer an issue
        if table_to_load in session_parallelization_tables and env in ['dev','qa']:
            logging.info("Replacing parallel hints in extract query with blank spaces")
            pattern = r'/\*\+\s*parallel\(\d+\)\s*\*/'
            srctablequery = re.sub(pattern, '', srctablequery)
            count_query = re.sub(pattern, '', count_query)
            logging.info("Running alt process to alter session to run query in parallel")
            with count_conn.cursor() as cursor:
                cursor.execute("ALTER SESSION FORCE PARALLEL QUERY PARALLEL 16")
                logging.info("Executed ALTER SESSION statement")
                cursor.execute("SELECT COUNT(*) FROM CONCUITY.APL_APPEAL")
                logging.info("Executed dummy query")
                cursor.execute(count_query)
                logging.info("Executed count query")
                res =cursor.fetchall()
                logging.info("Fetched results of count query")
                header = [i[0] for i in cursor.description]
                src_rec_count = pd.DataFrame(res, columns=header)['COUNT'].tolist()[0]
        else:
            src_rec_count = pd.read_sql(count_query, con=count_conn)['COUNT'].tolist()[0]
        count_conn.close()
        logging.info("Count for table {} from source is {}".format(srctablename,src_rec_count))
                
        if is_partitioned:        
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_partitions) as executor:
                tasks = {executor.submit(
                    process_partition, partition, srctablequery): partition for partition in range(num_partitions)}

                for completed_task in concurrent.futures.as_completed(tasks):
                    logging.info(completed_task)
                    partition = tasks[completed_task]
                    try:
                        logging.info('Getting completed task')
                        load_row_count = completed_task.result()
                        logging.info(load_row_count)
                        logging.info(
                            f"Rows processed in partition {partition}: {load_row_count}")
                        
                        load_count += load_row_count
                    except Exception as exc:
                        logging.error(f'An exception occurred: {exc}')
        else:
            load_count = process_partition(0, srctablequery)

        if table_to_load == 'mapcl_id':
            writeback_conn = conn_pool.acquire()
            with writeback_conn.cursor() as cursor:
                logging.info("Beginning truncate of MAPCL Miss")
                if env == 'dev':
                    cursor.execute(f"DELETE FROM HCA_REPORTING.MON_ACCOUNT_PAYER_CALC_MISS{bq_suffix.upper()} WHERE schema_id = {str(src_schema_id)*2}")
                    writeback_conn.commit()
                    logging.info("Finished truncate of MAPCL Miss")
                    load_calc_miss_to_oracle_dev('mapcl_id', 'mon_account_payer_calc_latest', f'mon_account_payer_calc_miss{bq_suffix}')
                elif env == 'qa':
                    cursor.execute(f"CALL HCA_REPORTING.TRUNC_MON_ACT_PYR_CLC_MS{bq_suffix.upper()}()")
                    writeback_conn.commit()
                    logging.info("Finished truncate of MAPCL Miss")
                    load_calc_miss_to_oracle('mapcl_id', 'mon_account_payer_calc_latest', f'mon_account_payer_calc_miss{bq_suffix}')
                else:
                    cursor.execute(f"CALL HCA_REPORTING.TRUNC_MON_ACT_PYR_CLC_MS{bq_suffix.upper()}()")
                    writeback_conn.commit()
                    logging.info("Finished truncate of MAPCL Miss")
                    load_calc_miss_to_oracle('mapcl_id', 'mon_account_payer_calc_latest', f'mon_account_payer_calc_miss{bq_suffix}')
            writeback_conn.close()
            # Commented for Calc Service as we have implemented New CDC
        # elif table_to_load == 'mapcl_svc_id':
        #     writeback_conn = conn_pool.acquire()
        #     with writeback_conn.cursor() as cursor:
        #         logging.info("Beginning truncate of MAPCL Srvc Miss")
        #         if env == 'dev':
        #             cursor.execute(f"DELETE FROM HCA_REPORTING.MON_ACCT_PAYER_CALC_SVC_MISS{bq_suffix.upper()} WHERE schema_id = {str(src_schema_id)*2}")
        #             writeback_conn.commit()
        #             logging.info("Finished truncate of MAPCL Srvc Miss")
        #             load_calc_miss_to_oracle_dev('mapcl_svc_id', 'mon_account_payer_calc_service', f'mon_acct_payer_calc_svc_miss{bq_suffix}')
        #         elif env == 'qa':
        #             cursor.execute(f"CALL HCA_REPORTING.TRUNC_MON_ACT_PYR_CLC_SVC_MS{bq_suffix.upper()}()")
        #             writeback_conn.commit()
        #             logging.info("Finished truncate of MAPCL Srvc Miss")
        #             load_calc_miss_to_oracle('mapcl_svc_id', 'mon_account_payer_calc_service', f'mon_acct_payer_calc_svc_miss{bq_suffix}')
        #         else:
        #             cursor.execute(f"CALL HCA_REPORTING.TRUNC_MON_ACT_PYR_CLC_SVC_MS{bq_suffix.upper()}()")
        #             writeback_conn.commit()
        #             logging.info("Finished truncate of MAPCL Srvc Miss")
        #             load_calc_miss_to_oracle('mapcl_svc_id', 'mon_account_payer_calc_service', f'mon_acct_payer_calc_svc_miss{bq_suffix}')
        #     writeback_conn.close()
        elif table_to_load == 'etl_cc_source_row_counts':
            etl_cc_source_rows_count_cleanup()
        
        conn_pool.close(force=True)
        
        tableload_end_time = str(pendulum.now(timezone))[:23]
        tableload_run_time = (pd.to_datetime(
            tableload_end_time) - pd.to_datetime(tableload_start_time))
        tgt_rec_count = load_count
        logging.info(tgt_rec_count)
        if src_rec_count == tgt_rec_count:
            audit_status = 'PASS'
        elif tgt_rec_count > src_rec_count:
            audit_status = 'PASS(More records in Target)'
        else:
            audit_status = 'FAIL'

        audit_insert_stt = "insert into {} values (GENERATE_UUID() , {}, '{}', '{}', '{}', '{}', {}, {}, '{}', '{}', '{}', '{}', '{}', '{}'  ) ".format(
            config['env']['v_parallon_ra_audit_dataset_name']+"."+ra_oracle['v_audittablename'],
            srctableid,
            sourcesysnm,
            srctablename,
            table_to_load_full,
            'RECORD_COUNT',
            src_rec_count,
            tgt_rec_count,
            tableload_start_time,
            tableload_end_time,
            tableload_run_time,
            jobname,
            str(pendulum.now(timezone))[:23],
            audit_status
        )

        audit_entry = pd.read_gbq(
            audit_insert_stt, project_id=bqproject_id, max_results=0)
        logging.info(
            "===Audit entry added for srctableid {} - srctablename {} ===".format(srctableid, srctablename))

        logging.info(
            "===Execute Validation SQL's if any for table  {} ===".format(table_to_load_full))
        self.executevalidationsqls(table_to_load)


    def process(self, element):

        try:
            global bqproject_id
            bqproject_id = config['env']['v_curated_project_id']
            tablelist = ra_oracle[src_tbl_list]

            # read input table list and process each table in sequence

            num_tables =  len(tablelist)
            logging.info("===Number of tables {}===".format(str(num_tables)))
        
            for tableinfo in tablelist:
                self.readjdbcwritebqtable(tableinfo, bqproject_id)

            logging.info("===END of processing tablelist at {}".format(
                time.strftime("%Y%m%d-%H:%M:%S")))

        except:
            logging.error(
                "===ERROR: Failure occurred within Process function===")
            logging.error(traceback.format_exc())
            raise SystemExit()


def run():
    pipeline_args = [
        "--project", config['env']['v_proc_project_id'],
        "--service_account_email", config['env']['v_df_atos_serviceaccountemail'],
        "--job_name", jobname,
        "--runner", config['env']['v_runner'],
        "--network", config['env']["v_network"],
        "--subnetwork", config['env']["v_subnetwork"],
        "--staging_location", config['env']["v_dfstagebucket"],
        "--temp_location", config['env']["v_gcs_temp_bucket"],
        "--region", config['env']["v_region"],
        "--save_main_session",
        "--num_workers", str(config['env']["v_numworkers"]),
        "--max_num_workers", str(config['env']["v_maxworkers"]),
        "--no_use_public_ips",
        "--dataflow_service_options, enable_prime",
        "--autoscaling_algorithm", "THROUGHPUT_BASED",
        "--worker_machine_type", ra_oracle['v_machine_type'],
        "--setup_file", '{}/setup.py'.format(utilpath)
    ]


    try:
        logging.info(
            "===Apache Beam Run with pipeline options started===")
        pcoll = beam.Pipeline(argv=pipeline_args)
        if config['env']['v_runner'] == 'DataflowRunner':
            pcoll | "Initialize" >> beam.Create(["1"]) | 'Setup Dataflow Worker' >> beam.ParDo(
                setuprunnerenv()) | "Read JDBC, Write to Google BigQuery" >> beam.ParDo(jdbctobq())
            logging.info("===Submitting Asynchronous Dataflow Job===")
            p = pcoll.run()
            dataflow_job_id = p.job_id()
        else:
            pcoll | "Initialize.." >> beam.Create(
                ["1"]) | "Read JDBC, Write to Google BigQuery" >> beam.ParDo(jdbctobq())
            logging.info("===Submitting Asynchronous Dataflow Job===")
            p = pcoll.run()
            dataflow_job_id = '123' #p.job_id()

        logging.info("===Submitted Asynchronous Dataflow Job, Job id is " + dataflow_job_id + " ====")
        return dataflow_job_id

    except:
        logging.exception("===ERROR: Apache Beam Run Failed===")
        raise SystemExit()


def main(sourcesysnm, src_tbl_list):
    global jobname
    randomstring = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
    if src_schema_id == '1':
        schema = 'p1'
    elif src_schema_id == '3':
        schema = 'p2'
    jobname = sourcesysnm + "-p-" + srcsys_config_file.split('.')[0].replace('_', '-') + '-' + src_tbl_list + '-' + schema + '-' + time.strftime("%Y%m%d%H%M%S") #+ '-' + randomstring

    logging.info("===Job Name is {} ===".format(jobname))
    p1 = run()
    dataflow_job_id = p1

    logging.info("===END: Data Pipeline for src_tbl_list {}-{} at {}===".format(
        sourcesysnm, src_tbl_list, time.strftime("%Y%m%d-%H:%M:%S")))
    # DO NOT REMOVE: Need for xcom 
    print(dataflow_job_id)

    return dataflow_job_id

# Run manually from local and dags folder with python scripts/Oracle_to_BigQuery_PythonTemplate_daily.py --src_sys_config_file=ra_oracle_ingest_dependency_daily.yaml --src_sys_airflow_varname=ra_oracle_ingest_dependency_daily --src_tbl_list=tblist14 --src_schema_id=3 
if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--src_sys_config_file", required=True,
                        help=("Source System Based Config file name"))
    parser.add_argument("--src_sys_airflow_varname", required=True,
                        help=("Source System Based Config file name"))
    parser.add_argument("--src_tbl_list", required=True,
                        help=("Source Table List"))
    parser.add_argument("--src_schema_id", required=True, 
                        help=("Oracle Instance ID"))
    # nargs argument allows for list to be passed as argument (each element needs to be separated by a space)
    parser.add_argument("--tables_to_ignore", required=True, nargs='*',
                        help=("Tables the script will ignore because a full pull job is being run instead"))
    args = parser.parse_args()

    global srcsys_config_file
    srcsys_config_file = args.src_sys_config_file

    global ra_oracle
    ra_oracle = call_config_yaml(args.src_sys_config_file, args.src_sys_airflow_varname)

    global src_tbl_list
    src_tbl_list = args.src_tbl_list

    global src_schema_id
    src_schema_id = args.src_schema_id

    global tables_to_ignore
    tables_to_ignore = args.tables_to_ignore

    global sourcesysnm
    sourcesysnm = ra_oracle['v_sourcesysnm'].replace('_','-')

    global bq_stage_dataset
    bq_stage_dataset = config['env']['v_parallon_ra_stage_dataset_name']

    logging.info("===BEGIN: Data Pipeline for src_tbl_list {}-{} at {}===".format(
        sourcesysnm, src_tbl_list, time.strftime("%Y%m%d-%H:%M:%S")))
    main(sourcesysnm, src_tbl_list)