import jaydebeapi
import pandas as pd
import logging
import json
import os

# host_name = 'erp1.es.medcity.net'
# user_name = 'DMXERP1'
# pass_word = 'dmxbasep'
# port = '452'
# db_instance = 'ERP1'
# src_db_type = 'db2'

# host_name = "CANNWPDBSRPT01.hca.corpad.net"
user_name = "PSC_GCP_User"
pass_word = "G4rfdV!#hu88TceR9"
src_db_type = 'sqlserver'
port = '1433'

jdbc_lib_path = r"C:\Users\KHU9683\OneDrive - HCA Healthcare\Merwin\Utilities\JarFiles" + "\\"
print("=== Lib Path {} ===".format(str(jdbc_lib_path)))

def get_jdbc_details(host_name, port, db_instance=''):
    if src_db_type == 'sqlserver':
        jdbc_url = f"jdbc:sqlserver://{host_name}:{port};encrypt=false;trustServerCertificate=true"
        jdbc_class_name = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
        jdbc_jar = 'mssql-jdbc-12.4.0.jre8.jar'
    elif src_db_type == 'cache':
        jdbc_url = f"jdbc:Cache://{host_name}:{port}/{db_instance}"
        jdbc_class_name = 'com.intersys.jdbc.CacheDriver'
        jdbc_jar = 'cache-jdbc-2.0.0.jar'
    elif src_db_type == 'db2':
        jdbc_url = f"jdbc:db2://{host_name}:{port}/{db_instance}"
        jdbc_class_name = 'com.ibm.db2.jcc.DB2Driver'
        jdbc_jar = 'db2jcc4.jar'
    elif src_db_type == 'oracle':
        jdbc_url = f"jdbc:oracle:thin:@//{host_name}:{port}/{db_instance}"
        jdbc_class_name = 'oracle.jdbc.driver.OracleDriver'
        jdbc_jar = 'ojdbc8.jar'
    elif src_db_type == 'teradata':
        jdbc_url = f"jdbc:teradata://{host_name}/database={db_instance},dbs_port={port}"
        jdbc_class_name = 'com.teradata.jdbc.TeraDriver'
        jdbc_jar = 'terajdbc4.jar'
    else:
        jdbc_url = f"jdbc:sqlserver://{host_name}:{port};encrypt=false;trustServerCertificate=true"
        jdbc_class_name = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
        jdbc_jar = 'mssql-jdbc-12.4.0.jre8.jar'
    return jdbc_url, jdbc_class_name, jdbc_jar


#path of file with table name and database name in csv file 
input_path = "InputFiles\\PSC_Table_List.csv"
output_path = "OutputFiles\\PSC_Missing_Columns.csv"

#Write file to local directory
def write_file_local(path,file_data):
    
    with open(path, 'w') as file:
        file_string = '\n'.join(file_data)
        file.write(file_string)

#get the DDLs for the tables from teradata 
def get_missing_columns():
    df = pd.read_csv(input_path, index_col=None)        
    table_info = []
    prev_server_name = ''
    for index, row in df.iterrows():
        try:
            server_name = str(row['Server']).strip().lower()
            if server_name != prev_server_name:
                jdbc_url, jdbc_class_name, jdbc_jar = get_jdbc_details(server_name, port)
                jdbc_jar_path = os.path.join(jdbc_lib_path, jdbc_jar)
                print("=== JDBC URL {} ===".format(str(jdbc_url)))
                print("=== Class {} ===".format(str(jdbc_class_name)))
                print("=== JAR {} ===".format(str(jdbc_jar)))
                print("=== JAR Path {} ===".format(str(jdbc_jar_path)))
                conn = jaydebeapi.connect(jdbc_class_name, jdbc_url, {'user': user_name, 'password': pass_word}, jdbc_jar_path, jdbc_lib_path)
                prev_server_name = server_name
            database_name = str(row['DatabaseName']).strip().lower()
            schema_name = str(row['Schema']).strip().lower()
            table_name = str(row['TableName']).strip().lower()
            view_name = str(row['View']).strip()

            tbl_df = None
            query = "USE {}; SELECT tb_col.column_name as tb_column_name, tb_col.data_type as tb_data_type, "\
                "vw_col.column_name as vw_column_name, vw_col.data_type as vw_data_type FROM "\
                "(SELECT column_name, data_type, ordinal_position FROM information_schema.columns "\
                "WHERE LOWER(table_schema) = 'gcp' AND LOWER(table_name) = 'vw_{}') vw_col "\
                "FULL OUTER JOIN "\
                "(SELECT column_name, data_type, ordinal_position FROM information_schema.columns "\
                "WHERE LOWER(table_schema) = '{}' AND LOWER(table_name) = '{}') tb_col"\
                "ON tb_col.column_name = vw_col.column_name "\
                "WHERE (tb_col.column_name IS NULL OR vw_col.column_name IS NULL) "\
                "ORDER BY vw_col.ordinal_position ASC, tb_col.ordinal_position ASC "\
                ";".format(database_name, view_name, schema_name, table_name)
            print(query)
            tbl_df = pd.read_sql(query, conn)
            if tbl_df.empty:
                table_present = False
            else:
                table_present = True

            for index, row in tbl_df.iterrows():
                table_info.append((',').join([database_name, schema_name, table_name, view_name, table_present, 
                                              row['tb_column_name'].strip(), row['tb_data_type'].strip(), 
                                              row['vw_column_name'].strip(), row['vw_data_type'].strip()]))

        except Exception as e1:
            print(e1)
            print("Database : {}, Table : {}".format(database_name,table_name))
            pass

    write_file_local(output_path, json.dumps(table_info))

print("Begin of Processing")

get_missing_columns()

print("End of Processing")