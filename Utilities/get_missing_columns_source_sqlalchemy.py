import pyodbc
import sqlalchemy
import pandas as pd

user_name = "PSC_GCP_User"
pass_word = "G4rfdV!#hu88TceR9"
src_db_type = 'sqlserver'
port = '1433'

def get_sql_alchemy_conn(user_name, pass_word, host_name, port, db_instance=''):
    if src_db_type == 'sqlserver':
        conn_url = f'mssql+pyodbc://{user_name}:{pass_word}@{host_name}:{port}/{db_instance}?driver=SQL+Server'
    # elif src_db_type == 'cache':
    #     conn_url = f"jdbc:Cache://{host_name}:{port}/{db_instance}"
    # elif src_db_type == 'db2':
    #     conn_url = f"jdbc:db2://{host_name}:{port}/{db_instance}"
    # elif src_db_type == 'oracle':
    #     conn_url = f"jdbc:oracle:thin:@//{host_name}:{port}/{db_instance}"
    elif src_db_type == 'teradata':
        conn_url = f'teradatasql://{host_name}/?user={user_name}&password={pass_word}&logmech=KRB5'
    else:
        conn_url = f'mssql+pyodbc://{user_name}:{pass_word}@{host_name}:{port}/{db_instance}?driver=SQL+Server'
    return conn_url


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
    prev_database_name = ''
    for index, row in df.iterrows():
        try:
            server_name = str(row['Server']).strip()
            database_name_table = str(row['DatabaseName_Table']).strip()
            if server_name.lower() != prev_server_name or database_name_table.lower() != prev_database_name_table:
                conn_url = get_sql_alchemy_conn(user_name, pass_word, server_name, port, database_name_table)
                # print(conn_url)
                db_engine = sqlalchemy.create_engine(conn_url)
                prev_server_name = server_name.lower()
                prev_database_name_table = database_name_table.lower()

            database_name_view = str(row['DatabaseName_View']).strip()
            schema_name = str(row['Schema']).strip()
            table_name = str(row['TableName']).strip()
            view_name = str(row['View']).strip()
            schema_base = str(row['Schema_Base']).strip().lower()

            if schema_base == "table":
                tbl_df = None
                query = "SELECT tb_col.column_name as tb_column_name, tb_col.data_type as tb_data_type, tb_col.is_nullable as tb_is_nullable, "\
                    "vw_col.column_name as vw_column_name, vw_col.data_type as vw_data_type, vw_col.is_nullable as vw_is_nullable FROM "\
                    "(SELECT column_name, data_type, is_nullable, ordinal_position FROM {}.information_schema.columns "\
                    "WHERE LOWER(table_schema) = 'gcp' AND LOWER(table_name) = 'vw_{}') vw_col "\
                    "FULL OUTER JOIN "\
                    "(SELECT column_name, data_type, is_nullable, ordinal_position FROM {}.information_schema.columns "\
                    "WHERE LOWER(table_schema) = '{}' AND LOWER(table_name) = '{}') tb_col "\
                    "ON tb_col.column_name = vw_col.column_name "\
                    "WHERE (tb_col.column_name IS NULL OR vw_col.column_name IS NULL) "\
                    "ORDER BY vw_col.ordinal_position ASC, tb_col.ordinal_position ASC "\
                    ";".format(database_name_view, view_name, database_name_table, schema_name, table_name)
                # print(query)
                tbl_df = pd.read_sql(query, db_engine)
                # print(tbl_df)

                for index, row in tbl_df.iterrows():
                    if row['tb_column_name']:
                        discrepancy_description = "Column Not Present in View"
                        missing_column = row['tb_column_name'].strip()
                        missing_column_datatype = row['tb_data_type'].strip()
                        missing_column_nullable = row['tb_is_nullable'].strip().lower()
                    else:
                        discrepancy_description = "Column Not Present in Table"
                        missing_column = row['vw_column_name'].strip()
                        missing_column_datatype = row['vw_data_type'].strip()
                        missing_column_nullable = row['vw_is_nullable'].strip().lower()
                    table_info.append((',').join([server_name, database_name_table, schema_name, table_name, database_name_view, view_name, discrepancy_description, 
                                                missing_column, missing_column_datatype, missing_column_nullable
                                                ]))
        
        except Exception as e1:
            print(e1)
            print("Query : {}".format(query))
            # pass

    write_file_local(output_path, table_info)

print("Begin of Processing")

get_missing_columns()

print("End of Processing")