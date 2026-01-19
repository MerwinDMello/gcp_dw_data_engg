import teradatasql
from google.cloud import bigquery
from tabulate import tabulate
import csv
import time
import getpass
import traceback
from datetime import datetime
import os
from google.cloud import bigquery
import pytz
import csv
import logging
import argparse
import getpass
import pandas as pd
import numpy as np
import pandas as pd

def sendmail(tolist, mailsubj, infilepath):
    import win32com.client as win32
    outlook = win32.Dispatch('Outlook.Application')
    mail = outlook.CreateItem(0)
    mail.Subject = mailsubj
    body = f"""\
        Hello,

        Please find attached the {mailsubj}. Let me know if you have any questions
        or require further information.

        Best regards,
        Ramu Pydimarri
        """

    mail.Body = body
    mail.To = tolist
    input_file = infilepath
    mail.Attachments.Add(input_file)
    mail.Send()
    print(f"Email Sent for {mailsubj}")

def get_table_row_count_teradata(src, table_name, td_where_clause, teradata_connection, cursor):

    query = f"SELECT cast(COUNT(1) as decimal(12,0)), current_timestamp td_min_lud,  current_timestamp td_max_lud FROM {table_name} {td_where_clause}  ;"
    cursor.execute(query)
    count, td_min_lud, td_max_lud = cursor.fetchone()

    if not os.path.isfile(td_key_cols_file):
        if  'base_views' in table_name:
            keycolsqry = f"SELECT columnname  FROM dbc.indicesV i where uniqueflag = 'Y'  and databasename||'_base_views.'|| tablename = '{table_name}' order by columnposition"
        else:
            keycolsqry = f"SELECT columnname  FROM dbc.indicesV i where uniqueflag = 'Y'  and databasename||'.'|| tablename = '{table_name}' order by columnposition"
        cursor.execute(keycolsqry)
        keycols = list(cursor.fetchall())
        keycolumns = [item.lower() for sublist in keycols for item in sublist]
        print("Read Unique keys from db - {}".format(str(keycolumns)))
    else:
        db = table_name.split('.')[0]
        if 'base_views' in db:
            db = db.split('_base_views')[0]
        tbl = table_name.split('.')[1]
        df = pd.read_csv(td_key_cols_file)
        filtered_df = df[(df['databasename'] == db) & (df['tablename'] == tbl)]
        keycolumns = filtered_df['columnname'].tolist()
        print("Read Unique keys from csv  for table {} - {} ".format(tbl, str(keycolumns)))


    return count, td_min_lud, td_max_lud, keycolumns





def get_table_row_count_bigquery(src, project_id, dataset_id, table_name, bq_where_clause, client):

    try:
        query = f"SELECT cast(COUNT(1) as numeric), current_timestamp() bq_min_lud , current_timestamp() bq_max_lud FROM `{project_id}.{dataset_id}.{table_name}` {bq_where_clause}  ;"
        query_job = client.query(query)
        count, bq_min_lud, bq_max_lud = query_job.result(
        ).to_dataframe().iloc[0]

        return count, bq_min_lud, bq_max_lud

    except Exception as e:
        print("Error while running count for {}.{}.{}".format(
            project_id, dataset_id, table_name))
        return 0, None, None


def get_mirr_table_row_count_bigquery(src, project_id, dataset_id, table_name, bq_where_clause, client):

    try:
        query = f"SELECT cast(COUNT(1) as numeric), current_timestamp() bq_min_lud , current_timestamp() bq_max_lud FROM `{project_id}.{dataset_id}.{table_name}` {bq_where_clause}  ;"
        query_job = client.query(query)
        count, bq_min_lud, bq_max_lud = query_job.result(
        ).to_dataframe().iloc[0]

        return count, bq_min_lud, bq_max_lud

    except Exception as e:
        print("Error while running count for {}.{}.{}".format(
            project_id, dataset_id, table_name))
        return 0, None, None





def get_audit_values(src, project_id, dataset_id, table_name, client):

    if 'base_views' in dataset_id:
        audit_dataset_id = dataset_id.split('_base_views')[0]
    elif 'staging' in dataset_id:
        audit_dataset_id = dataset_id.split('_staging')[0]
    else:
        audit_dataset_id = dataset_id

    if 'base_views' in dataset_id:
        dataset_id = dataset_id.split('_base_views')[0]

    try:
        if 'parallon' in project_id or 'edwcr' in dataset_id:
            query = f"select cast(COUNT(1) as numeric) num_audit_rows, cast(sum(expected_value)  as numeric) rowcount_expected_value, cast(sum(actual_value)  as numeric) rowcount_actual_value, min(audit_time)min_audit_time, max(audit_time) max_audit_time \
                from `{project_id}.{audit_dataset_id}_ac.audit_control` t1 where   cast(audit_time as date) = current_date and src_sys_nm  = '{src}' and tgt_tbl_nm = '{dataset_id}.{table_name}' ; "
        else:
            query = f"select cast(COUNT(1) as numeric) num_audit_rows, cast(sum(rowcount_expected_value)  as numeric) rowcount_expected_value, cast(sum(rowcount_actual_value)  as numeric) rowcount_actual_value, min(audit_time)min_audit_time, max(audit_time) max_audit_time \
                from `{project_id}.{audit_dataset_id}_ac.audit_control` t1 where   cast(audit_time as date) = current_date and src_sys_nm  = '{src}' and tgt_tbl_nm = '{dataset_id}.{table_name}' ; "
        query_job = client.query(query)
        num_audit_rows, rowcount_expected_value, rowcount_actual_value, min_audit_time, max_audit_time = query_job.result(
        ).to_dataframe().iloc[0]

        return num_audit_rows, rowcount_expected_value, rowcount_actual_value, min_audit_time, max_audit_time

    except Exception as e:
        print("Error while running audit for {}.{}.{}".format(
            project_id, dataset_id, table_name))
        print("Query : ", query)
        return 0, 0, 0, None, None


def generate_except_query(project_id, dataset_id, table_name, ignore_cols, bq_where_clause, mir_where_clause,  client, mirr_dataset):

    bq_where_clause = bq_where_clause.replace("'", "\\'")
    mir_where_clause = mir_where_clause.replace("'", "\\'")


    if 'base_views' in dataset_id:
        dataset_id = dataset_id.split('_base_views')[0]

    try:

        ex_query = f'''
        SELECT
  'select coalesce(sum(cnt),0)  diff_qry_result  from ( select count(*) cnt from ( select  bq_mir.* from ( select ' || STRING_AGG(
    CASE
      WHEN data_type = 'DATETIME' THEN 'datetime_trunc('|| column_name || ', SECOND)' || column_name
      WHEN data_type = 'STRING' THEN 'trim(upper(' || column_name || ')) ' || column_name
    ELSE
    column_name
  END
    , ', '
  ORDER BY
    ordinal_position) || ' from  `' || table_catalog || '`.' || table_schema || '.' || table_name || ' {bq_where_clause} ' || ' except distinct  select ' || STRING_AGG(
    CASE
      WHEN data_type = 'DATETIME' THEN 'datetime_trunc('|| column_name || ', SECOND)' || column_name
      WHEN data_type = 'STRING' THEN 'trim(upper(' || column_name || '))' || column_name
    ELSE
    column_name
  END
    , ', '
  ORDER BY
    ordinal_position) || ' from  `' ||  '{project_id}`.{mirr_dataset}.' || table_name || ' {mir_where_clause} ' || ') bq_mir union all select  mir_bq.* from ( select '|| STRING_AGG(
    CASE
      WHEN data_type = 'DATETIME' THEN 'datetime_trunc('|| column_name || ', SECOND)' || column_name
      WHEN data_type = 'STRING' THEN 'trim(upper(' || column_name || '))' || column_name
    ELSE
    column_name
  END
    , ', '
  ORDER BY
    ordinal_position) || ' from  `' || table_catalog || '`.' || '{mirr_dataset}' || '.' || table_name || ' {mir_where_clause} ' || ' except distinct select '|| STRING_AGG(
    CASE
      WHEN data_type = 'DATETIME' THEN 'datetime_trunc('|| column_name || ', SECOND)'
      WHEN data_type = 'STRING' THEN 'trim(upper(' || column_name || '))'
    ELSE
    column_name
  END
    , ', '
  ORDER BY
    ordinal_position) || ' from  `' || table_catalog || '`.' || table_schema || '.' || table_name || ' {bq_where_clause} ' || ') mir_bq ) ) ;' AS except_query
FROM
  `{project_id}`.{dataset_id}.INFORMATION_SCHEMA.COLUMNS
WHERE
  table_name = '{table_name}'
  AND column_name NOT IN ({ignore_cols})
GROUP BY
  table_catalog,
  table_schema,
  table_name;
   '''

        ex_query_job = client.query(ex_query)
        #print(ex_query)
        except_query = ex_query_job.result(
        ).to_dataframe().iloc[0]['except_query']
        #print(except_query)

        except_query_job = client.query(except_query)
        except_query_result = except_query_job.result(
        ).to_dataframe().iloc[0]['diff_qry_result']
        # print(except_query)
        return except_query_result, except_query

    except Exception as e:
        traceback.print_exc()
        print(ex_query)

        print("Error while running query for {}.{}.{}".format(
            project_id, dataset_id, table_name))
        return 0, None

    # return except_query



def generate_column_check_query(project_id, dataset_id, table_name, ignore_cols, bq_where_clause, mir_where_clause, td_keycols, client, mirr_dataset):

    bq_where_clause = bq_where_clause.replace("'", "\\'")
    mir_where_clause = mir_where_clause.replace("'", "\\'")

    if 'base_views' in dataset_id:
        dataset_id = dataset_id.split('_base_views')[0]

    try:
        cc_query = f'''
WITH
  bq_columns AS (
  SELECT
    CONCAT('`',table_catalog,'.',table_schema,'.',table_name,'`') AS full_name,
    column_name,
    data_type,
    ordinal_position
  FROM
    `{project_id}`.{dataset_id}.INFORMATION_SCHEMA.COLUMNS
  WHERE
    table_name = '{table_name}' ),
  mir_columns AS (
  SELECT
    CONCAT('`',table_catalog,'.',table_schema,'.',table_name,'`') AS full_name,
    column_name,
    data_type,
    ordinal_position
  FROM
    `{project_id}`.{mirr_dataset}.INFORMATION_SCHEMA.COLUMNS
  WHERE
    table_name = '{table_name}' )
SELECT
  CONCAT('\
    with deduped_landing_src as ( \
            select *, \
                row_number() over ( partition by {", ".join(td_keycols)} ) as rn \
            from ', b.full_name, \
            ' {bq_where_clause} ', ') ', ', \
        deduped_landing_tgt as ( \
            select *, \
                row_number() over (partition by {", ".join(td_keycols)}) as rn \
            from ',a.full_name, \
            ' {mir_where_clause} ', ') ', '\
    select  count(*) as total_count, \
            sum(case when {"or ".join(['a.'+key_col + ' is not null ' for key_col in td_keycols])} \
                    then 1 else 0 end) as target_only_count, \
            sum( case when {"or ".join(['b.'+key_col + ' is not null ' for key_col in td_keycols])} \
                    then 1 else 0 end) as source_only_count, \
            ', STRING_AGG(CONCAT('sum(case when ', \
        CASE \
            WHEN a.data_type=b.data_type  \
                THEN CASE \
                        WHEN a.data_type = 'STRING'\
                        THEN CONCAT('trim(upper(a.',a.column_name,')) = trim(upper(b.', b.column_name, '))  \
                            or ( a.',a.column_name,' is null and b.', b.column_name,' is null )')  \
                        WHEN a.data_type = 'DATETIME'\
                        THEN CONCAT('datetime_trunc(a.',a.column_name,', SECOND) = datetime_trunc(b.', b.column_name, ', SECOND) \
                            or ( a.',a.column_name,' is null and b.', b.column_name,' is null )') \
                        ELSE CONCAT('a.',a.column_name,' = b.', b.column_name, '  \
                            or ( a.',a.column_name,' is null and b.', b.column_name,' is null )') \
                        END \
            ELSE CONCAT('cast(a.',a.column_name,' as ',b.data_type,') = b.', b.column_name, '\
                or ( a.',a.column_name,' is null and b.', b.column_name,' is null )') \
        END \
        , ' then 0 else 1 end) as non_matching_',a.column_name),','), ' \
        from deduped_landing_tgt as a \
        full outer join deduped_landing_src as b \
        on {" and ".join(['a.'+key_col +'='+'b.'+key_col for key_col in td_keycols])} \
        and b.rn=1 \
        and a.rn=1' \
        ) AS cc_query
FROM
  bq_columns a
JOIN
  mir_columns b
ON
  a.column_name=b.column_name
GROUP BY
  a.full_name,
  b.full_name ;
        '''
        #print(cc_query)
        cc_query_job = client.query(cc_query)
        count_check_query = cc_query_job.result().to_dataframe().iloc[0]['cc_query']

        count_check_query_job = client.query(count_check_query)
        #print(count_check_query)
        count_check_query_results = count_check_query_job.result()
        schema = [field.name for field in count_check_query_results.schema]

        first_row = next(count_check_query_results, None)
        if first_row:
            formatted_row = '|'.join([f"{header}:{getattr(first_row, header)}" for header in schema])
        else:
            formatted_row = ''

        return count_check_query, formatted_row

    except Exception as e:
        traceback.print_exc()
        print(cc_query)

        print("Error while running query for {}.{}.{}".format(
            project_id, dataset_id, table_name))
        return 0, None

    # return except_query


if __name__ == "__main__":

    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument('-i', '--input_file', required=True,
                        help=("Provide Input Table List file name"))

    parser.add_argument('-g', '--gcp_project_id', required=True,
                        help=("Provide GCP Project ID"))

    parser.add_argument('-u', '--td_user', required=True,
                        help=("Provide Teradadta User"))

    parser.add_argument('-v', '--data_validation_results_table', required=False,
                        help=("data_validation_results Table Full Name eg hca-hin-dev-cur-clinical.edwcdm_ac.data_validation_results"))

    parser.add_argument('-e', '--email_list', required=False,
                        help=("Semi Colon Separated Email List"))

    parser.add_argument('-w', '--td_password', required=False,
                        help=("Teradata Password"))

    args = parser.parse_args()

    teradata_db_params = {
        'host': 'EDWPROD.DW.MEDCITY.NET',
        'user': None,
        'password':  None,
        'logmech': 'LDAP'
    }

    teradata_db_params['user'] = args.td_user
    teradata_db_params['password'] = args.td_password

    if not teradata_db_params['password']:
        teradata_db_params['password'] = getpass.getpass(
            prompt="Enter your Teradata password: ")

    project_id = args.gcp_project_id
    input_file_name = args.input_file
    tolist = args.email_list

    start_time = time.time()
    basefolder = '/opt/tdgcp/utilities/'
    td_key_cols_file_name = 'td_unique_keys.csv'
    td_key_cols_file = os.path.join(basefolder,td_key_cols_file_name)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    output_file_name = input_file_name.split('.')[0]
    output_file_name = f'{output_file_name}_out_{timestamp}.csv'
    data_validation_results_table = args.data_validation_results_table

    table_names = []

    try:
        with open(os.path.join(basefolder,  input_file_name), 'r') as file:
            for line in file:
                stripped_line = line.strip()
                if stripped_line and not stripped_line.startswith('#'):
                    table_names.append(stripped_line)

    except FileNotFoundError:
        print(f"File '{input_file_name}' not found. Exiting.")
        exit(1)

    result_table = []

    try:
        with teradatasql.connect(**teradata_db_params) as teradata_connection:
            with teradata_connection.cursor() as teradata_cursor:
                bigquery_client = bigquery.Client(project=project_id)

                for table_name in table_names:
                    try:
                        parts = table_name.split('~')
                        src, in_teradata_schema, in_teradata_table, in_bigquery_dataset, in_bigquery_table, in_ignore_cols, in_bq_where_clause, in_mir_where_clause, in_comments, in_td_where_clause = parts
                        if not in_bigquery_dataset:
                            bigquery_dataset = in_teradata_schema
                        else: 
                            bigquery_dataset = in_bigquery_dataset

                        if not in_bigquery_table:
                            bigquery_table = in_teradata_table
                        else:
                            bigquery_table = in_bigquery_table

                        if not in_td_where_clause:
                            in_td_where_clause = 'where 1 = 1'
                        else:
                            in_td_where_clause = in_td_where_clause

                        if not in_bq_where_clause:
                           bq_where_clause = ' where 1 = 1 '
                        else:
                           bq_where_clause = in_bq_where_clause

                        if not in_mir_where_clause:
                           mir_where_clause = bq_where_clause
                        else:
                           mir_where_clause = in_mir_where_clause

                        if 'staging' in in_teradata_schema:
                                mod_teradata_schema = in_teradata_schema
                        else:
                                mod_teradata_schema = in_teradata_schema + '_base_views'

                        td_count, td_min_lud, td_max_lud, td_keycols = get_table_row_count_teradata(src,
                                                                                        mod_teradata_schema + '.' + in_teradata_table, in_td_where_clause,  teradata_connection, teradata_cursor)
                        bq_count, bq_min_lud, bq_max_lud = get_table_row_count_bigquery(src,
                                                                                        project_id, bigquery_dataset, bigquery_table, bq_where_clause, bigquery_client)

                        td_bq_count_diff = int(td_count) - int(bq_count)
                        if td_count != 0:
                            percentage_diff = round(
                                (td_bq_count_diff / td_count) * 100, 2)
                        else:
                            percentage_diff = 0

                        percentage_diff = str(percentage_diff)

                        if td_count == bq_count:
                            td_bq_status = 'Pass'
                        else:
                            td_bq_status = 'Fail'

                        num_audit_rows, rowcount_expected_value, rowcount_actual_value, min_audit_time, max_audit_time = get_audit_values(
                            src, project_id, bigquery_dataset, bigquery_table, bigquery_client)

                        if not rowcount_expected_value and not rowcount_actual_value:
                            audit_status = 'NO-Audit'
                        elif 'AUDIT_SKIP' in in_comments.upper():
                            audit_status = 'Skip-Audit'
                        elif rowcount_expected_value == rowcount_actual_value:
                            audit_status = 'Pass'
                        else:
                            audit_status = 'Fail'

                        if 'staging' in in_teradata_schema:
                            table_type = 'staging'
                        else:
                            table_type = 'core'

                        if td_bq_status == 'Pass' and audit_status == 'Pass':
                            overall_status = f'Pass-{table_type}'
                        elif td_bq_status == 'Pass' and audit_status != 'Pass':
                            overall_status = f'Pass-{table_type}=>audit:{audit_status}'
                        else:
                            overall_status = f"Fail-{table_type}=>td_bq:{td_bq_status}, audit:{audit_status}"
                        table = {
                            'src': src,
                            'td_schema': in_teradata_schema,
                            'td_table': in_teradata_table,
                            'bq_dataset': in_bigquery_dataset,
                            'bq_table': in_bigquery_table,
                            'ignore_cols': in_ignore_cols,
                            'bq_where_clause': in_bq_where_clause,
                            'mir_where_clause': in_mir_where_clause,
                            'comments': in_comments,
                            'td_count': int(td_count),
                            'bq_count': int(bq_count),
                            'td_bq_count_diff': td_bq_count_diff,
                            'td_bq_percentage_diff': percentage_diff,
                            'td_bq_status': td_bq_status,
                            'td_min_dw_lud': td_min_lud,
                            'td_max_dw_lud': td_max_lud,
                            'bq_min_dw_lud': bq_min_lud,
                            'bq_max_dw_lud': bq_max_lud,
                            'mirr_count': None,
                            'mirr_bq_count_diff': None,
                            'mirr_percentage_diff': None,
                            'mirr_bq_status': None,
                            'mirr_min_lud': None,
                            'mirr_max_lud': None,
                            'diff_qry_result': None,
                            'diff_qry_percentage_diff': None,
                            'diff_qry_status': None,
                            'diff_qry': None,
                            'num_audit_rows': num_audit_rows,
                            'expected_value': rowcount_expected_value,
                            'actual_value': rowcount_actual_value,
                            'audit_status': audit_status,
                            'min_audit_time': min_audit_time,
                            'max_audit_time': max_audit_time,
                            'overall_status': overall_status,
                            'count_check_query': None,
                            'count_check_query_results': None,
                            'sys_time': None
                        }

                        if "-dev-" in project_id:
                            mirr_dataset = f'{bigquery_dataset}_base_views'
                        else:
                            mirr_dataset = f'{bigquery_dataset}_base_views_copy'
                        
                        if 'staging' not in mirr_dataset:
                            mirr_count, mirr_min_lud, mirr_max_lud = get_mirr_table_row_count_bigquery(src,
                                                                                                  project_id, mirr_dataset, bigquery_table, mir_where_clause, bigquery_client)
                            table['mirr_count'] = mirr_count
                            table['mirr_min_lud'] = mirr_min_lud
                            table['mirr_max_lud'] = mirr_max_lud

                            mirr_bq_count_difference = int(
                                mirr_count) - int(bq_count)
                            table['mirr_bq_count_diff'] = mirr_bq_count_difference

                            if mirr_count != 0:
                                mirr_percentage_diff = round(
                                    (mirr_bq_count_difference / mirr_count) * 100, 2)
                            else:
                                mirr_percentage_diff = 0

                            mirr_percentage_diff = str(mirr_percentage_diff)

                            table['mirr_percentage_diff'] = mirr_percentage_diff

                            if mirr_count == bq_count:
                                mirr_bq_status = 'Pass'
                            else:
                                mirr_bq_status = 'Fail'
                            table['mirr_bq_status'] = mirr_bq_status

                            if not in_ignore_cols:
                                ignore_cols = "'dw_last_update_date_time'"
                            else:
                                ignore_cols = f"'dw_last_update_date_time',{in_ignore_cols}"

                            if not in_bq_where_clause:
                                bq_where_clause = ' where 1 = 1 '
                            else:
                                bq_where_clause = in_bq_where_clause

                            if not in_mir_where_clause:
                                mir_where_clause = bq_where_clause
                            else:
                                mir_where_clause = in_mir_where_clause

                            diff_qry_result, diff_qry = generate_except_query(
                                project_id, bigquery_dataset, bigquery_table, ignore_cols, bq_where_clause, mir_where_clause, bigquery_client, mirr_dataset)
                            table['diff_qry_result'] = diff_qry_result

                            count_check_query, count_check_query_results = generate_column_check_query(project_id, bigquery_dataset, bigquery_table, ignore_cols, bq_where_clause, mir_where_clause, td_keycols, bigquery_client, mirr_dataset)
                            table['count_check_query'] = count_check_query
                            table['count_check_query_results'] = count_check_query_results

                            if bq_count != 0:
                                diff_qry_percentage_diff = round(
                                    (diff_qry_result / bq_count) * 100, 2)
                            else:
                                diff_qry_percentage_diff = 0

                            diff_qry_percentage_diff = str(
                                diff_qry_percentage_diff)

                            table['diff_qry_percentage_diff'] = diff_qry_percentage_diff

                            if diff_qry_result == 0:
                                diff_qry_status = 'Pass'
                            else:
                                diff_qry_status = 'Fail'

                            table['diff_qry_status'] = diff_qry_status
                            table['diff_qry'] = diff_qry

                            if overall_status == 'Pass' and mirr_bq_status == 'Pass' and diff_qry_status == 'Pass':
                                overall_status = f"Pass-{table_type}"
                            elif overall_status != 'Pass' and mirr_bq_status == 'Pass' and diff_qry_status == 'Pass':
                                overall_status = f"Pass-{table_type}=>audit:{audit_status}"
                            else:
                                overall_status = f"Fail-{table_type}=>td_bq:{td_bq_status}, audit:{audit_status}, mirr_bq:{mirr_bq_status}, diff_qry:{diff_qry_status}"

                            table['overall_status'] = overall_status


                        sys_time = datetime.now(pytz.timezone(
                            'America/Chicago')).strftime("%Y-%m-%dT%H:%M:%S")
                        table['sys_time'] = sys_time

                        result_table.append(table)
                        # print(table)

                    except Exception as e:
                        traceback.print_exc()
                        print(f"Error: {e}")

                if result_table:
                    sorted_result_table = sorted(
                        result_table, key=lambda x: (x['td_schema'], x['overall_status']))

                    headers = sorted_result_table[0].keys()
                    data = [row.values() for row in sorted_result_table]
                    tabulated_data = tabulate(data, headers, tablefmt="pretty")
                    # print(tabulated_data)

                    with open(os.path.join(
                            basefolder,  'output', output_file_name), 'w', newline='') as csv_file:
                        csv_writer = csv.writer(csv_file)
                        csv_writer.writerow(headers)
                        csv_writer.writerows(data)

                    # print("Tabulated data for {} has been saved to {}".format(table, output_file_name))

                    if tolist:
                        sendmail(tolist,
                                 f"Data Validation Test results - {output_file_name}", os.path.join(
                                     basefolder,  'output', output_file_name))

                    if data_validation_results_table:
                        results_gcp_project_id = data_validation_results_table.split('.')[
                            0]
                        results_dataset_id = data_validation_results_table.split('.')[
                            1]
                        results_table_name = data_validation_results_table.split('.')[
                            2]

                        df = pd.read_csv(os.path.join(
                            basefolder,  'output', output_file_name))

                        df.replace({np.nan: None}, inplace=True)
                        df = df.astype(str)

                        client = bigquery.Client(
                            project=results_gcp_project_id)
                        dataset_ref = client.dataset(results_dataset_id)
                        table_ref = dataset_ref.table(results_table_name)

                        try:
                            table = client.get_table(table_ref)
                            print(
                                f'Table {results_dataset_id}.{results_table_name} already exists.')
                        except:
                            print(
                                f'Table {results_dataset_id}.{results_table_name} does not exist. Creating...')

                            schema = []
                            for column in df.columns:
                                schema.append(
                                    bigquery.SchemaField(column, 'STRING'))

                            table = bigquery.Table(table_ref, schema=schema)
                            table = client.create_table(table)

                        job_config = bigquery.LoadJobConfig(
                            write_disposition='WRITE_APPEND',
                            autodetect=True,
                        )

                        job = client.load_table_from_dataframe(
                            df, table_ref, job_config=job_config)
                        job.result()
                        print(
                            f'Loaded {job.output_rows} row(s) into {results_dataset_id}.{results_table_name}')

    except teradatasql.DatabaseError as teradata_error:
        print(f"Teradata Error: {teradata_error}")

    except Exception as bigquery_error:
        traceback.print_exc()
        print(f"BigQuery Error: {bigquery_error}")

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution Time: {execution_time:.2f} seconds")

