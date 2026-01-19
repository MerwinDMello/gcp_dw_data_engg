import pandas as pd
import teradatasql
import json
import csv

f = open('config.json')
sp_doc = pd.read_csv(r"Stored_Procedures_List.csv", header = None)

# print(sp_doc)

creds = json.load(f)

SP_List = sp_doc[0]
# print(SP_List)


# Function Exports the SQL files after cleaning. I plan on making this it's own function later.
# Function to find and replace specific strings. Add anything you would like to find and replace.
def script_clean(output):
    print("called function")
    output = output.replace("END REQUEST;", " ")
    output = output.replace("BEGIN REQUEST", " ")
    output = output.replace("BEGIN", " ")
    
    # Trims 3 characters off of the beginning. Stores the files.
    with open(f"./PI_Stored_Procedures_CDDPI107 - Final Metrics/{df.RequestText[1]}.sql", "w", encoding="utf-8") as file:
                file.write(output[3:-4])


            
            
# Loop through the CSV list of Stored Procedures.
for procedures in SP_List:
    print(procedures)
    
#     Replace with your credentials
    with teradatasql.connect(host='edwprod.dw.medcity.net', user='GFA5095', password=creds["p"], logmech="LDAP") as connect:

        query = f"""show procedure {procedures}"""

        #Reading query to df
        df = pd.read_sql(query,connect)
        
        
#       Check to see if the number of rows in the dataframe is greater than 3
#       I plan on making this section cleaner, but for now it works with what we need. 

        if df.shape[0] == 4:
                        
            output = df['RequestText'][2] + df['RequestText'][3]
            print( f"This script is lengthy {df.shape[0]}")
            script_clean(output)


        elif df.shape[0] == 5:
                        
            output = df['RequestText'][2] + df['RequestText'][3] + df['RequestText'][4]
            script_clean(output)
            
            print( f"This script is VERY lengthy {df.shape[0]}")
            
        elif df.shape[0] == 6:

            output = df['RequestText'][2] + df['RequestText'][3] + df['RequestText'][4] + df['RequestText'][5] 
            script_clean(output)

            print( f"This script is SUPER lengthy {df.shape[0]}")

        elif df.shape[0] == 7:
                        
            output = df['RequestText'][2] + df['RequestText'][3] + df['RequestText'][4] + df['RequestText'][5] + df['RequestText'][6] 
            script_clean(output)
            
            print( f"This script is SUPER lengthy {df.shape[0]}")
            
        elif df.shape[0] == 8:
                        
            output = df['RequestText'][2] + df['RequestText'][3] + df['RequestText'][4] + df['RequestText'][5] + df['RequestText'][6] + df['RequestText'][7]
            script_clean(output)
            
            print( f"This script is SUPER lengthy {df.shape[0]}")
            
        elif df.shape[0] == 9:
                        
            output = df['RequestText'][2] + df['RequestText'][3] + df['RequestText'][4] + df['RequestText'][5] + df['RequestText'][6] + df['RequestText'][7]+ df['RequestText'][8]
            script_clean(output)
            
            print( f"This script is SUPER lengthy {df.shape[0]}")
        
        
            
        elif df.shape[0] >= 10:
            
            print("!!!!!!!!!!!ATTENTION, THIS SCRIPT IS TOO LONG TO CONVERT!!!!!!!!!!")


        else:

        # Remove portions of code that cause errors when converting to BQ SQL.
            output = df['RequestText'][2]
            script_clean(output)



