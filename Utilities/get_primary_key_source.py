import jaydebeapi
import pandas as pd
import logging

host_name = 'erp1.es.medcity.net'
user_name = 'DMXERP1'
pass_word = 'dmxbasep'
port = '452'
db_instance = 'ERP1'
src_db_type = 'db2'

# host_name = "CANNWPDBSRPT01.hca.corpad.net"
# user_name = "CANNSVCCDMETL"
# pass_word = "smock-=c2w"
# src_db_type = 'sqlserver'
# port = '1433'

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
jdbc_lib_path = 'C:\\Merwin\\Utilities\\JarFiles\\'
print("=== JDBC URL {} ===".format(str(jdbc_url)))
print("=== Class {} ===".format(str(jdbc_class_name)))
print("=== Lib Path {} ===".format(str(jdbc_lib_path)))
print("=== JAR {} ===".format(str(jdbc_jar)))

conn = jaydebeapi.connect(jdbc_class_name, jdbc_url, {'user': user_name, 'password': pass_word}, jdbc_lib_path + jdbc_jar, jdbc_lib_path)

#path of file with table name and database name in csv file 
input_path = "InputFiles\\tables_get_rec_counts_source.csv"
output_path = "OutputFiles\\tables_primary_keys_source.csv"

#Write file to local directory
def write_file_local(path,file_data):
    
    with open(path, 'w') as file:
        file_string = '\n'.join(file_data)
        file.write(file_string)

#get the DDLs for the tables from teradata 
def get_rec_counts_table_updates():
    df = pd.read_csv(input_path, index_col=None)        
    table_rec_counts = []
    for index, row in df.iterrows():
        try:

            database_name = str(row['Database']).strip().lower()
            table_name = str(row['Table']).strip().lower()

            query = "SELECT LOWER(name) AS COLUMNNAME, LOWER(coltype) AS COLUMNTYPE, keyseq AS KEYSEQ "\
                "From SYSIBM.SYSCOLUMNS WHERE LOWER(TBCREATOR) = '{}' AND LOWER(TBNAME) = '{}'"\
                ";".format(database_name, table_name)
            cols_df = pd.read_sql(query, conn)

            primary_keys_df = cols_df[cols_df['KEYSEQ'] > 0].sort_values(by=['KEYSEQ'])

            for index, row in primary_keys_df.iterrows():
                table_rec_counts.append((',').join([database_name, table_name, row['COLUMNNAME'], row['COLUMNTYPE'].strip()]))

        except Exception as e1:
            print(e1)
            print("Database : {}, Table : {}".format(database_name,table_name))
            pass

    write_file_local(output_path, table_rec_counts)
    print("Tables for Record Counts : ", len(df))

print("Begin of Processing")

get_rec_counts_table_updates()

print("End of Processing")