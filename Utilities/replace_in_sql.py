#Import packages
import pandas as pd
import numpy as np

#List of replacements
change_df = [
    {'search':'DECLARE current_ts timestamp;','replace':'DECLARE current_ts datetime;'},
    {'search':'timestamp_trunc(current_timestamp()', 'replace':'timestamp_trunc(current_datetime(\'US/Central\')'},
    {'search':'TIMESTAMP_TRUNC(current_timestamp()','replace':'timestamp_trunc(current_datetime(\'US/Central\')'},
    {'search':'TIMESTAMP("9999-12-31 23:59:59+00")','replace':'DATETIME("9999-12-31 23:59:59")'}
]

#list of SQLs
sql_file_list = [ 'hdw_kronos_time_entry.sql', 'hdw_kronos_ref_clock.sql']
for sql_file in sql_file_list:
        print(sql_file)
        new_file = ""    
        sql_file = open(sql_file, 'r+')
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
        sql_file.write(new_file)
        sql_file.close()