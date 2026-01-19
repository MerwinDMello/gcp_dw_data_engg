import teradatasql
import pandas as pd

#Change credentials and file path
user_name = 'JQE4491'
pword = 'Atos@123'
file_path = "C:\\Users\\JQE4491\\Desktop\\Documents\\Lawson SQL"

#path of file with table name and database name in csv file 
control_path = "tables.csv"

#Write file to local directory
def write_file_local(path,file_data):
    
    with open(path, 'a') as file:
        file_string = '\n'.join(file_data)
        file.write(file_string)


#get the DDLs for the tables from teradata 
def get_ddl() -> int:
    with teradatasql.connect(host='EDWPROD.DW.MEDCITY.NET', user=user_name, password=pword, logmech="LDAP") as connect:
        df = pd.read_csv(control_path, index_col=None)        
        ddl, error = 0,0
        failed_tables = []
        for index, row in df.iterrows():
            file_data_core, file_data_staging, file_data_view = [],[],[]
            if row['Database'] == 'EDWHR_BASE_VIEWS':
                query = "SHOW VIEW "+row['Database']+"."+row['Table']
            else:
                query = "SHOW TABLE "+row['Database']+"."+row['Table']

            #print(query)        

            try:
                results_df = pd.read_sql(query, connect)
                #print(results_df)
                if row['Database'] == 'EDWHR':
                    file_data_core.append("\n/*"+row['Database']+"."+row['Table']+"*/\n")
                    file_data_core.append(results_df['Request Text'][0])  
                    #Write to core file
                    path = file_path+"\\Core\\"+ row['Table'].lower()+".sql"
                    write_file_local(path,file_data_core)     
    
                elif row['Database'] == 'EDWHR_BASE_VIEWS':  
                    file_data_view.append("\n/*"+row['Database']+"."+row['Table']+"*/\n")
                    file_data_view.append(results_df['Request Text'][0])   
                    # Write to view file
                    path = file_path+"\\Base_View\\"+row['Table'].lower()+".sql"
                    write_file_local(path,file_data_view)  
    
                else:
                    file_data_staging.append("\n/*"+row['Database']+"."+row['Table']+"*/\n")
                    file_data_staging.append(results_df['Request Text'][0])
                    print(file_data_staging)
                    # Write to staging file
                    path = file_path+"\\Staging\\"+row['Table'].lower()+".sql"
                    write_file_local(path,file_data_staging)
    
                ddl+=1
    
            except Exception as e1:
                print("Could not get DDL for",row['Table'])            
                error+=1
                failed_tables.append(row['Database']+"."+row['Table'])
                pass
        print("# tables in csv: ", len(df))
        print("# tables with ddl: ", ddl)
        print("# tables without ddl: ", error)
        print("List of tables without ddl ", failed_tables)

 
print("Begin of Processing")

get_ddl()


print("End of Processing")