import teradatasql
import pandas as pd
from google.cloud import bigquery
import pandas_gbq
import io,os
import json
import logging
logging.getLogger().setLevel(logging.DEBUG)
import google.auth
import numpy as np
import math

os.environ["HTTP_PROXY"] = "proxy.nas.medcity.net:80"
os.environ["HTTPS_PROXY"] = "proxy.nas.medcity.net:80"

credentials, _ = google.auth.default()
credentials = google.auth.credentials.with_scopes_if_required(credentials, bigquery.Client.SCOPE)
authed_http = google.auth.transport.requests.AuthorizedSession(credentials)


bq_project_id = 'hca-hin-dev-cur-hr'
bq_dataset = 'edwhr_copy'
tgt_bq_table = 'test_ref_survey'
host, username, password, driver = 'edwprod.dw.medcity.net', '<user>', '<password>', 'Teradata Database ODBC Driver 17.10'
srctablequery = """
    select * from edwhr_base_views.ref_survey
    """
v_chunksize = 100000

#localbasepath = "C:\\Users\\LDO4585\\Documents\\rp_docs\\teradata_ddls\\"
with teradatasql.connect(host='EDWPROD.DW.MEDCITY.NET', user=username, password=password, logmech="LDAP") as connect:
    bqclient = bigquery.Client(project=bq_project_id,credentials=credentials, _http=authed_http)
    job_config = bigquery.job.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    dataset_ref = bqclient.dataset(bq_dataset)
    table_ref = dataset_ref.table(tgt_bq_table)
    table = bqclient.get_table(table_ref)
    logging.info(table.schema)
    f = io.StringIO("")
    bqclient.schema_to_json(table.schema, f)
    tblschema = json.loads(f.getvalue())
    logging.info(tblschema)
    tgt_bq_table = bq_dataset + '.' + tgt_bq_table

    col_spl_char = ['.', '[', ']']
    load_count = 0

    for chunk in pd.read_sql(srctablequery, connect, chunksize=v_chunksize):
        chunk.columns = map(str.lower, chunk.columns)
        print(chunk.head(10))
        for char in col_spl_char:
            chunk.columns = chunk.columns.str.replace(char, '_', regex=False)

    #chunk["dw_last_update_date_time"] = dt.now()
   # print(chunk.dtypes)

    # Change column name and datatype of dataframe
        for i in range(0, len(tblschema)):
            logging.info(tblschema[i])
            print(tblschema[i])
            dtype = tblschema[i]['type'].lower()
            print(dtype)

            if dtype == 'numeric':
                dtype = 'object'
            elif dtype == 'date' or dtype == 'datetime':
                dtype = 'datetime64'
                chunk[tblschema[i]['name']]= chunk[tblschema[i]['name']].astype(str).str.replace('9999-12-31','2250-12-31', regex=False)
            elif dtype == 'integer':
                dtype = 'Int64'

            chunk[tblschema[i]['name']]= chunk[tblschema[i]['name']].replace("None", np.nan, regex=False)

            if i < len(chunk.columns):
                chunk[tblschema[i]['name']] = chunk[tblschema[i]
                                                    ['name']].astype(dtype)
            else:
                chunk[tblschema[i]['name']] = None
                chunk[tblschema[i]['name']] = chunk[tblschema[i]
                                                    ['name']].astype(dtype)

        pandas_gbq.to_gbq(chunk, tgt_bq_table,
                          project_id=bq_project_id, if_exists='append', table_schema=tblschema)

        load_count += len(chunk.index)
        logging.info("==={} rows loaded into table {}===".format(
            load_count, tgt_bq_table))
