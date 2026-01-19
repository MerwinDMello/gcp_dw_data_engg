# !gcloud config set project hca-hin-dev-cur-parallon

import os
import subprocess
import logging
import re
import pandas as pd
from datetime import datetime
os.environ["HTTP_PROXY"] = "proxy.nas.medcity.net:80"
os.environ["HTTPS_PROXY"] = "proxy.nas.medcity.net:80"
user_name = "PSC_GCP_User"
password = "G4rfdV!#hu88TceR9"

mssql_dir=r"C:\Users\KHU9683\AppData\Roaming\Python\Python313\Scripts"

input_path = r"InputFiles\PSC_Table_List.csv"
output_path = r"C:\Users\KHU9683\OneDrive - HCA Healthcare\Merwin\SQLServer"
pattern_fmt_code = r"\d+"

def run_mssql_scripter_command(server, database_name, schema, table_name, server_num):
    # command = f'gsutil -m cp -r "{source}" {destination}'
    command = f'mssql-scripter -S {server} -d {database_name} -U {user_name} -P {password} --script-create --include-types Table --exclude-use-database --exclude-headers --exclude-defaults --exclude-indexes --exclude-triggers --exclude-extended-properties --exclude-check-constraints --file-per-object --include-schemas {schema} --include-objects {table_name}  -f "{output_path}\\{server_num}"'
    print(command)
    try:
        completed_process = subprocess.run(
            command, cwd=mssql_dir, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        print(completed_process.stdout)
    except subprocess.CalledProcessError as e:
        print("Error:", e)
        print(e.output)

dt1 = datetime.now()
source_df = pd.read_csv(input_path, index_col=None)
for index, row in source_df.iterrows():
    server = row['Server']
    database_name = row['DatabaseName_Table']
    schema = row['Schema']
    table_name = row['TableName']
    view_name = row['View']
    merge_fields = row['Merge_Field']
    schema_base = str(row['Schema_Base']).strip().lower()
    if schema_base == "table":
        server_num_match = re.search(pattern_fmt_code, server, re.IGNORECASE)
        server_num = server_num_match.group().lower()
        run_mssql_scripter_command(server, database_name, schema, table_name, server_num)
        target_file_path = f"{output_path}\\{server_num}\\{schema}.{table_name}.Table.sql"
        new_target_file_path = f"{output_path}\\{server_num}\\{view_name}.sql"
        if os.path.exists(target_file_path):
            os.rename(target_file_path, new_target_file_path)
            # Read the file content
            with open(new_target_file_path, 'r') as file:
                file_content = file.read()

            # Perform the replacement
            old_string = f"[{schema}].[{table_name}]"
            search_pattern = re.compile(re.escape(old_string), re.IGNORECASE)
            new_string = f"[{view_name}]"
            modified_content = search_pattern.sub(new_string, file_content)

            # Write the modified content back to the file
            with open(new_target_file_path, 'w') as file:
                file.write(modified_content)
dt2 = datetime.now()
print(dt2-dt1)