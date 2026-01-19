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
output_path = "OutputFiles\\PSC_DW_Last_Update_Columns.csv"

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
    prev_database_name_view = ''
    for index, row in df.iterrows():
        try:
            server_name = str(row['Server']).strip()
            database_name_table = str(row['DatabaseName_Table']).strip()
            database_name_view = str(row['DatabaseName_View']).strip()
            if server_name.lower() != prev_server_name or database_name_view.lower() != prev_database_name_view:
                conn_url = get_sql_alchemy_conn(user_name, pass_word, server_name, port, database_name_view)
                # print(conn_url)
                db_engine = sqlalchemy.create_engine(conn_url)
                prev_server_name = server_name.lower()
                prev_database_name_view = database_name_view.lower()

            schema_name = str(row['Schema']).strip()
            table_name = str(row['TableName']).strip()
            view_name = str(row['View']).strip()
            process_type = str(row['Process']).strip()

            tbl_df = None
            query = "SELECT "\
                "column_name, data_type "\
                "FROM information_schema.columns "\
                "WHERE LOWER(table_schema) = 'gcp' AND LOWER(table_name) = 'vw_{}' "\
                "AND LOWER(column_name) IN ('DWLastUpdateDateTime', 'DWLastUpdatedDate', 'DWLastUpdatedDateTime') "\
                ";".format(view_name)
            # print(query)
            tbl_df = pd.read_sql(query, db_engine)
            # print(tbl_df)

            DWLastUpdateDateTime_Present = False
            DWLastUpdateDateTime_Datatype = None
            DWLastUpdatedDate_Present = False
            DWLastUpdatedDate_Datatype = None
            DWLastUpdatedDateTime_Present = False
            DWLastUpdatedDateTime_Datatype = None

            for index, row in tbl_df.iterrows():
                match str(row['column_name'].lower()):
                    case "dwlastupdatedatetime":
                        DWLastUpdateDateTime_Present = True
                        DWLastUpdateDateTime_Datatype = str(row['data_type'])
                    case "dwlastupdateddate":
                        DWLastUpdatedDate_Present = True
                        DWLastUpdatedDate_Datatype = str(row['data_type'])
                    case "dwlastupdateddatetime":
                        DWLastUpdatedDateTime_Present = True
                        DWLastUpdatedDateTime_Datatype = str(row['data_type'])
            table_info.append((',').join([server_name, database_name_table, schema_name, table_name, database_name_view, view_name, process_type,
                                          str(DWLastUpdateDateTime_Present), str(DWLastUpdateDateTime_Datatype),
                                          str(DWLastUpdatedDate_Present), str(DWLastUpdatedDate_Datatype),
                                          str(DWLastUpdatedDateTime_Present), str(DWLastUpdatedDateTime_Datatype)
                                            ]))
        
        except Exception as e1:
            print(e1)
            print("Query : {}".format(query))
            # pass

    write_file_local(output_path, table_info)

print("Begin of Processing")

get_missing_columns()

print("End of Processing")