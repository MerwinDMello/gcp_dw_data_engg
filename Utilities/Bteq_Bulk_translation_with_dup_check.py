# gcloud auth application-default login

import paramiko
from google.cloud import storage
import pandas as pd
from google.cloud import bigquery_migration_v2
from google.cloud import bigquery
import google.auth
import re
import json
import teradatasql
import time
import sqlparse

import os
os.environ["HTTP_PROXY"] = "proxy.nas.medcity.net:80"
os.environ["HTTPS_PROXY"] = "proxy.nas.medcity.net:80"

#Set all the required parameters
#Set 'Yes' to load to VM
post = 'No'

#Set 'Yes" to load to GCS Bucket
load = 'Yes'

#Set 'Yes' to trigger migration workflow
migrate = 'Yes'

#Set 'Yes' to modify BTEQs
modify_bteq = 'Yes'

file_path = 'C:\\Merwin\\Utilities\\InputFiles\\'
bucket_staging_path = ""
bucket_core_path = ""
bucket_name = "eim-cs-da-gmek-5764-dev"
gcs_input_path = 'HRG/Bteq_Source_Files/MERWIN/Enwisen/'
gcs_output_path = 'HRG/Bteq_Converted_Files/MERWIN/Enwisen/'
# gcs_input_path = 'bteq_migration/bteq_source/edwhr/'
# gcs_output_path = 'bteq_migration/bteq_converted/edwhr'
display_name = "HR_Enwisen_BTEQ_Conversion_AutoTrigger_Merwin"
project_id = 'hca-hin-dev-cur-hr'
remote_bteq_path = '/etl/jfmd/EDWHR/Scripts/'
remote_file_path = ''
vm_location = '' 
user_name = 'KHU9683'
pword = 'Mer82@dme'

#List of BTEQs/Source Files to copy
bteqs = [
]

if len(bteqs) == 0:
    df = pd.read_csv(file_path + "bteqs.csv", index_col=None)
    bteqs = df["bteqs"]
    print(bteqs)

source_files =[
]

#List of replacements
change_df = [
    {'search':'__DEFAULT_DATABASE__.`$ncr_stg_schema.`','replace':'{{ params.param_hr_stage_dataset_name }}.'},
    {'search':'__DEFAULT_DATABASE__.edwhr_staging.','replace':'{{ params.param_hr_stage_dataset_name }}.'},
    {'search':'__DEFAULT_DATABASE__.`$ncr_tgt_schema.`','replace':'{{ params.param_hr_core_dataset_name }}.'},
    {'search':'__DEFAULT_DATABASE__.edwhr.', 'replace':'{{ params.param_hr_core_dataset_name }}.'},
    {'search':'__DEFAULT_DATABASE__.edwhr_base_views.', 'replace':'{{ params.param_hr_base_views_name }}.'},
    {'search':'__DEFAULT_DATABASE__.edw_pub_views.', 'replace':'{{ params.param_pub_views_name }}.'},
    {'search':'__DEFAULT_DATABASE__.edwhr_views.', 'replace':'{{ params.param_hr_views_name }}.'},
    {'search':'__DEFAULT_DATABASE__.edw_dim_base_views.', 'replace':'{{ params.param_dim_base_views_name }}.'},
    {'search':'__DEFAULT_DATABASE__.edwhr_dim.', 'replace':'{{ params.param_hr_dim_core_name }}.'},
    {'search':'__DEFAULT_DATABASE__.edwhr_bi_views.', 'replace':'{{ params.param_hr_bi_views_name }}.'},
    {'search':'__DEFAULT_DATABASE__.edwhr_dmx_ac.', 'replace':'{{ params.param_hr_audit_dataset_name }}.'},
    {'search':'__DEFAULT_DATABASE__.edwhr_dmx_ac_base_views.', 'replace':'{{ params.param_hr_audit_dataset_name }}.'},
    {'search':'__DEFAULT_DATABASE__.edwhr_ac.', 'replace':'{{ params.param_hr_audit_dataset_name }}.'},
    {'search':'current_date()','replace':'current_ts'},
    {'search':'\'9999-12-31\'','replace':'DATETIME("9999-12-31 23:59:59")'},
    {'search':'timestamp_trunc(current_timestamp(), SECOND)','replace':'timestamp_trunc(current_datetime(\'US/Central\'), SECOND)'},
    {'search':'CALL edwhr_procs.sk_gen(\'EDWHR_STAGING\'','replace':'CALL `{{ params.param_hr_core_dataset_name }}`.sk_gen("{{ params.param_hr_stage_dataset_name }}"'},

]

keywords =['INSERT', 'DELETE', 'UPDATE', 'MERGE', 'TRUNCATE','SEL', 'INS', 'edwhr_procs.sk_gen', 'insert', 'delete', 'update', 'merge', 'truncate','sel', 'ins' ]

param_list='[{"params.param_hr_core_dataset_name":"edwhr"}, {"params.param_hr_stage_dataset_name":"edwhr_staging"}]'
params = json.loads(param_list)
srcfile_list = []
bteq_list = []

def copy_bteq_files_to_local():
    print("Begin: File copy from Unix Server to Local")

    ssh_client=paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(hostname='naetlp11.unix.medcity.net',username=user_name,password=pword)
    ftp_client=ssh_client.open_sftp()

    for bteq in bteqs:
        if '$' in bteq:
            file_list = ftp_client.listdir(path=remote_bteq_path)
            for file in file_list:
                is_pattern = re.search(bteq, file, re.I)
                if is_pattern:
                    bteq_list.append(file)
                    remote_location =  remote_bteq_path + file
                    file_location = file_path + "BTEQ\\" + file
                    ftp_client.get(remote_location, file_location)
        else:
            bteq_list.append(bteq)
            remote_location =  remote_bteq_path + bteq
            file_location = file_path + "BTEQ\\" +bteq
            ftp_client.get(remote_location, file_location)
    print(bteq_list)
    print(f'BTEQ copied to  location {file_path} BTEQ\\')


    for srcfile in source_files:
        if '$' in srcfile:
            file_list = ftp_client.listdir(path=remote_file_path)
            for file in file_list:
                is_pattern = re.search(srcfile, file, re.I)
                if is_pattern:
                    srcfile_list.append(file)
                    remote_location =  remote_file_path + file
                    file_location = file_path + "Source_Files\\" + file
                    ftp_client.get(remote_location, file_location)
        else:
            srcfile_list.append(srcfile)
            remote_location = remote_file_path + srcfile
            file_location = file_path + "Source_Files\\" + srcfile
            ftp_client.get(remote_location, file_location)
    print(srcfile_list)
    print(f'Src Files copied to specified location {file_path} Source_Files\\')

    ftp_client.close()
    print(f'End: File copy from Unix Server to Local')

def copy_files_to_vm():
    print("Begin: Copy file from Local to VM:xrdclpapphin02a")
    ssh_client=paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(hostname='xrdclpapphin02a.unix.medcity.net',username=user_name,password=pword)

    ftp_client=ssh_client.open_sftp()
    print(ftp_client.listdir())
    for file in srcfile_list:
        file_location = file_path + "BTEQ\\" + file
        ftp_client.put(file_location,vm_location)

    ftp_client.close()
    print("End: File has been copied to VM")

def upload_blob(bucket_name, file_path, gcs_path):
    print(f"Begin: Upload files to Bucket:{bucket_name}")
    bucket = gcsclient.bucket(bucket_name)

    for bteq in bteq_list:
        file_location = file_path + "BTEQ\\" + bteq
        destination_blob_name = gcs_path + bteq.lower()
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(file_location)
        print(f"File {file_location} uploaded to {destination_blob_name}.")
    
    print(f"End: Files are uploaded to:{gcs_path}")

def download_blob(bucket_name, file_path, gcs_path):
    print(f"Begin: Download Files from Bucket:{bucket_name}")    
    bucket = gcsclient.bucket(bucket_name)

    for bteq in bteq_list:
        source_blob_name = gcs_path +  bteq.lower()
        destination_file_name = file_path + "Converted_SQL\\" + bteq.lower()
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)

        print(
        "Downloaded storage object {} from bucket {} to local file {}.".format(
            source_blob_name, bucket_name, destination_file_name
        )
    )
    print(f"End: Files are downloaded to:{file_path}")

def create_migration_workflow(gcs_input_path: str, gcs_output_path: str, project_id: str) -> None:
    """Creates a migration workflow of a Batch SQL Translation and prints the response."""
    
    print(f"Begin: Create a Migration Workflow") 

    parent = f"projects/{project_id}/locations/us"

     # Construct a BigQuery Migration client object.
    client = bigquery_migration_v2.MigrationServiceClient()

    # Set the source dialect to Teradata SQL.
    source_dialect = bigquery_migration_v2.Dialect()
    source_dialect.teradata_dialect = bigquery_migration_v2.TeradataDialect(
        mode=bigquery_migration_v2.TeradataDialect.Mode.BTEQ
    )

    # Set the target dialect to BigQuery dialect.
    target_dialect = bigquery_migration_v2.Dialect()
    target_dialect.bigquery_dialect = bigquery_migration_v2.BigQueryDialect()
    
    # Prepare the config proto.
    translation_config = bigquery_migration_v2.TranslationConfigDetails(
        gcs_source_path= "gs://" + bucket_name + '/' + gcs_input_path,
        gcs_target_path="gs://" + bucket_name + '/' + gcs_output_path,
        source_dialect=source_dialect,
        target_dialect=target_dialect
    )

    # Prepare the task.
    migration_task = bigquery_migration_v2.MigrationTask(
        type_="Translation_Teradata2BQ", translation_config_details=translation_config
    )

    # Prepare the workflow.
    workflow = bigquery_migration_v2.MigrationWorkflow(
        display_name=display_name
    )

    workflow.tasks["translation-task"] = migration_task  # type: ignore

    # Prepare the API request to create a migration workflow.
    request = bigquery_migration_v2.CreateMigrationWorkflowRequest(
        parent=parent,
        migration_workflow=workflow,
    )
    response = client.create_migration_workflow(request=request)

    time.sleep(180)
 


    print(f"End: Query translation completed") 

def format_sql_remove_comments():

    print(f"Begin: Format SQL and Remove comments") 
    sql_file_list = os.listdir(file_path + "Converted_SQL")

    #Remove comments
    for sql_file in sql_file_list:
        new_file = ""
        sql_file_path = file_path + "Converted_SQL\\" + sql_file   
        print(sql_file_path)    
        with open(sql_file_path, 'r+') as sql_file: 
            text = sql_file.read() 
            new_file = sqlparse.format(text, strip_comments=True)
           

            sql_file.truncate(0)
            sql_file.seek(0)
            sql_file.writelines(new_file)
            sql_file.close()

    print(f"End: Format SQL & Remove Comments") 

def remove_bq_irrelavant_commands():

    print(f"Begin: Remove SQL statements that are not BQ relavent") 
    sql_file_list = os.listdir(file_path + "Converted_SQL")

    #Remove BQ irrelavant lines
    for sql_file in sql_file_list:
        new_file = ""
        new_file = "BEGIN\nDECLARE DUP_COUNT INT64;\nDECLARE current_ts datetime;\nSET current_ts = timestamp_trunc(current_datetime(\'US/Central\'), SECOND);" 
        sql_file_path = file_path + "Converted_SQL\\" + sql_file   
        print(sql_file_path)    
        with open(sql_file_path, 'r+') as sql_file: 
            text = sql_file.read() 
            lines = text.split(';')
            for stmnt in lines:
                if any(keyword in stmnt for keyword in keywords):
                    new_file += stmnt +";\n"
            new_file += "END;"
            sql_file.truncate(0)
            sql_file.seek(0)
            sql_file.writelines(new_file)
            sql_file.close()

    print(f"End: Remove SQL statements that are not BQ relavent") 
    
def find_and_replace():

    print(f"Begin: Find and replace") 
    sql_file_list = os.listdir(file_path + "Converted_SQL")                
    #Find and Replace
    for sql_file in sql_file_list:
            new_file = "" 
            sql_file_path = file_path + "Converted_SQL\\" + sql_file   
            sql_file = open(sql_file_path, 'r+')

            for line in sql_file:
                read_line = line.strip()

                for i in range(len(change_df)):
                    if i == 0:
                        #Searches for the value from search_col and replaces that with replace_col
                        new_line = read_line.replace(change_df[i]["search"], change_df[i]['replace'])      
                    else:
                        new_line = new_line.replace(change_df[i]["search"], change_df[i]['replace'])   

                new_file += new_line +"\n" 
            sql_file.truncate(0)
            sql_file.seek(0)
            sql_file.write(new_file)
            sql_file.close()
    print(f"End: Find and replace") 

def get_unique_index(database, table):
    with teradatasql.connect(host='EDWPROD.DW.MEDCITY.NET', user=user_name, password=pword, logmech="LDAP") as connect:
        query = f"SELECT columnname FROM dbc.indicesV where databasename = '{database}' and tablename = '{table}' and uniqueflag = 'Y'"
        try:
            results_df = pd.read_sql(query, connect)
        except Exception as e1:
            print("Exception occured when getting unique index")            
            pass
    return results_df

def add_dup_check():
    
    print(f"Begin: Add statement for checking duplicates") 
    sql_file_list = os.listdir(file_path + "Converted_SQL")

    #Add duplicate Check
    for sql_file in sql_file_list:
        new_file = ""
        sql_file_path = file_path + "Converted_SQL\\" + sql_file   

        with open(sql_file_path, 'r+') as sql_file: 
            text = sql_file.read() 
            lines = text.split(';')
            for stmnt in lines:
                if ("INSERT" in stmnt or "MERGE" in stmnt):
                    param_dataset = stmnt.split()[3]
                    table = re.sub('}}.', '',stmnt.split()[4])
                    for param in params:
                        if param_dataset in param:
                            dataset = param.get(f'{param_dataset}')
                            column_df = get_unique_index(dataset, table)
                    if not column_df.empty:
                        col = ''
                        for column in column_df['ColumnName']:
                            col = col + column.lower() + ','

                        dup_query = "SET DUP_COUNT = (\nselect count(*)\nfrom (\nselect\n"+ col[:-1] +"\nfrom {{"+ param_dataset + "}}." + table + "\n group by " + col[:-1] + "\nhaving count(*) > 1\n)\n);\nIF DUP_COUNT <> 0 THEN\nROLLBACK TRANSACTION;\nRAISE USING MESSAGE = concat('Duplicates are not allowed in the table');\nELSE\nCOMMIT TRANSACTION;\nEND IF;"
          
                        new_file += "BEGIN TRANSCATION;\n"
                        new_file += stmnt + ";\n"
                        new_file += dup_query
                    else:
                        new_file += stmnt + ";\n"
                else:
                        new_file += stmnt + ";\n"
            sql_file.truncate(0)
            sql_file.seek(0)
            sql_file.write(new_file)
            sql_file.close()
    print(f"End: Add statement for checking duplicates") 


print("Begin of Processing")
#credentials = google.auth.default()
#credentials = google.auth.credentials.with_scopes_if_required(credentials, bigquery.Client.SCOPE)
#authed_http = google.auth.transport.requests.AuthorizedSession(credentials)
#bqclient = bigquery.Client(project=project_id,credentials=credentials, _http=authed_http)
bqclient = bigquery.Client(project=project_id)
gcsclient = storage.Client(project=project_id)
#Step 1: Download sql/file from Unix Box(naetlp11) to Local
copy_bteq_files_to_local()

#Step 2: Upload source files to VM
if post == 'No':
    copy_files_to_vm()

#Step 3: Load file to GCS Bucket
if load == "Yes":
    upload_blob(bucket_name,file_path,gcs_input_path)

if migrate == "Yes":
    create_migration_workflow(gcs_input_path, gcs_output_path, project_id)
    download_blob(bucket_name, file_path, gcs_output_path)


if modify_bteq == 'Yes':
    format_sql_remove_comments()
    remove_bq_irrelavant_commands()
    find_and_replace()
    add_dup_check()

print("End of Processing")

