import jaydebeapi
import pandas as pd
import logging
from datetime import datetime, time

host_name = 'xrpswpdbsbiz16.hca.corpad.net'
user_name = 'PARASVCSQLCLMRMT'
pass_word = 'ow+O.7P@q4TVkd2MO6Qr}v[w@M+M1e'
port = '1433'
db_instance = 'AdmitToRemit'
src_db_type = 'sqlserver'

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

output_path = "OutputFiles\\secondary_billing_patients.csv"

#Write file to local directory
def write_file_local(path,file_data):
    
    with open(path, 'w') as file:
        file_string = '\n'.join(file_data)
        file.write(file_string)

#get the DDLs for the tables from teradata 
def get_rec_counts_table_updates():
    try:
        run_time = datetime.now().time()
        check_time = time(20,00)
        print("Run Time : {}".format(run_time))
        print("Check Time : {}".format(check_time))
        if run_time > check_time:
            day_ind = 0
        else:
            day_ind = -1

        source_query = "Select DISTINCT PAS_COID, Unit_Num, Patient_Acct_Num, IPlan, IPlan2 from AdmitToRemit.dbo.SecondaryBillingBatching nolock where DATEDIFF(day,getdate() ,CreatedDateTime) = {}".format(day_ind)
        print(source_query)

        table_rec_counts = []

        source_query_df = pd.read_sql(source_query, conn)
        print(source_query_df.columns.values)
        table_rec_counts.append((',').join(source_query_df.columns.values))

        for index, row in source_query_df.iterrows():
            table_rec_counts.append((',').join([row['PAS_COID'].strip(), row['Unit_Num'].strip(),
            row['Patient_Acct_Num'].strip(), row['IPlan'].strip(), row['IPlan2'].strip()]))

        write_file_local(output_path, table_rec_counts)
        print("Tables for Record Counts : ", len(source_query_df))

    except Exception as e1:
        print(e1)
        print("Database : {}".format(source_query))
        pass

print("Begin of Processing")

get_rec_counts_table_updates()

print("End of Processing")