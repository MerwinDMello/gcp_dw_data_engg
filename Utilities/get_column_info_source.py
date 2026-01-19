import jaydebeapi
import pandas as pd
import logging
import json

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

jdbc_lib_path = 'C:\\Merwin\\Utilities\\JarFiles\\'
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
output_path = "OutputFiles\\PSC_Table_Info.json"

#Write file to local directory
def write_file_local(path,file_data):
    
    with open(path, 'w') as file:
        # file_string = '\n'.join(file_data)
        file.write(file_data)

#get the DDLs for the tables from teradata 
def get_column_info():
    df = pd.read_csv(input_path, index_col=None)        
    table_info = []
    prev_server_name = ''
    for index, row in df.iterrows():
        try:
            server_name = str(row['Server']).strip().lower()
            if server_name != prev_server_name:
                jdbc_url, jdbc_class_name, jdbc_jar = get_jdbc_details(server_name, port)
                print("=== JDBC URL {} ===".format(str(jdbc_url)))
                print("=== Class {} ===".format(str(jdbc_class_name)))
                print("=== JAR {} ===".format(str(jdbc_jar)))
                conn = jaydebeapi.connect(jdbc_class_name, jdbc_url, {'user': user_name, 'password': pass_word}, jdbc_lib_path + jdbc_jar, jdbc_lib_path)
                prev_server_name = server_name
            database_name = str(row['DatabaseName']).strip().lower()
            schema_name = str(row['Schema']).strip().lower()
            table_name = str(row['TableName']).strip().lower()
            view_name = str(row['View']).strip()

            tbl_df = None
            query = "USE {}; SELECT t.object_id FROM sys.tables t "\
                "INNER JOIN sys.schemas s ON s.schema_id = t.schema_id "\
                "WHERE LOWER(s.Name) = '{}' AND LOWER(t.Name) = '{}'"\
                ";".format(database_name, schema_name, table_name)
            tbl_df = pd.read_sql(query, conn)
            if tbl_df.empty:
                table_present = False
            else:
                table_present = True

            cols_df = None
            query = "USE {}; SELECT column_name, data_type FROM information_schema.columns "\
                "WHERE LOWER(table_schema) = '{}' AND LOWER(table_name) = '{}' "\
                "ORDER BY ordinal_position ASC"\
                ";".format(database_name, schema_name, table_name)
            cols_df = pd.read_sql(query, conn)
            if cols_df.empty:
                columns_present = False
            else:
                columns_present = True
                
            ind_df = None
            index_present = False
            index_constraint_present = False
            if table_present == True:
                if len(tbl_df) == 1:
                    tbl_object_id = tbl_df["object_id"][0]
                    query = "USE {}; SELECT ind.index_id FROM sys.indexes ind "\
                        "WHERE ind.object_id = '{}' "\
                        "AND ind.is_primary_key = '1' "\
                        "AND ind.is_disabled = '0'"\
                        "AND ind.is_hypothetical = '0'"\
                        ";".format(database_name, tbl_object_id)
                    
                    ind_df = pd.read_sql(query, conn)
                    if ind_df.empty:
                        index_present = False
                    else:
                        index_present = True

                if index_present == False:
                    query = "USE {}; SELECT ind.index_id FROM sys.indexes ind "\
                        "WHERE ind.object_id = '{}' "\
                        "AND ind.is_unique = '1' "\
                        "AND ind.is_unique_constraint = '1' "\
                        "AND ind.is_disabled = '0'"\
                        "AND ind.is_hypothetical = '0'"\
                        ";".format(database_name, tbl_object_id)
                    
                    ind_df = pd.read_sql(query, conn)
                    if ind_df.empty:
                        index_present = False
                    else:
                        if len(ind_df) == 1:
                            index_present = True
                            index_constraint_present = True
                        else:
                            index_present = False

                if index_present == False:
                    query = "USE {}; SELECT ind.index_id FROM sys.indexes ind "\
                        "WHERE ind.object_id = '{}' "\
                        "AND ind.is_unique = '1' "\
                        "AND ind.is_disabled = '0'"\
                        "AND ind.is_hypothetical = '0'"\
                        ";".format(database_name, tbl_object_id)
                    
                    ind_df = pd.read_sql(query, conn)
                    if ind_df.empty:
                        index_present = False
                    else:
                        if len(ind_df) == 1:
                            index_present = True
                        else:
                            index_present = False

            idx_cols_df = None
            idx_columns_present = False
            if index_present == True:
                index_id = ind_df["index_id"][0]
                query = "USE {}; SELECT col.name as column_name, ic.key_ordinal as idx_position FROM sys.index_columns ic "\
                    "INNER JOIN sys.columns col ON ic.object_id = col.object_id and ic.column_id = col.column_id "\
                    "WHERE ic.object_id = '{}' AND ic.index_id = '{}' "\
                    "ORDER BY ic.key_ordinal ASC"\
                    ";".format(database_name, tbl_object_id, index_id)
                idx_cols_df = pd.read_sql(query, conn)
                if idx_cols_df.empty:
                    idx_columns_present = False
                else:
                    idx_columns_present = True

            if idx_columns_present == True:
                fields_df = cols_df.merge(idx_cols_df, on='column_name', how='left').fillna(0)
                fields_df["idx_position"] = fields_df["idx_position"].astype(int)
                fields_df['idx_position'] = fields_df['idx_position'].replace({0: None})
                json_output_records = fields_df.to_json(orient='records')
            else:
                json_output_records = cols_df.to_json(orient='records')

            table_info.append({"name": view_name, 
                               "table_present": table_present, 
                               "columns_present": columns_present,
                               "unique_index_present": index_present,
                               "unique_index_constraint_present": index_constraint_present,
                               "unique_index_columns_present": idx_columns_present,
                               "columns": json.loads(json_output_records)})

        except Exception as e1:
            print(e1)
            print("Database : {}, Table : {}".format(database_name,table_name))
            pass

    write_file_local(output_path, json.dumps(table_info, indent=3))

print("Begin of Processing")

get_column_info()

print("End of Processing")