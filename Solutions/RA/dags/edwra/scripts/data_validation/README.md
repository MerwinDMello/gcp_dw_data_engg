## Data Validation Script Instructions
### Steps
- Download following files to local computer:
    - data_validation_ra_qa.py
    - ra_table_list_daily.txt
    - td_lud_tbls.csv
    - td_unique_keys.csv
- Make the following replacements in the data_validation_ra_qa.py file 
    - Change gcp_project_id to the project you are running this script for (line 613)
    - Replace the variable 'basefolder' with the path on the local computer these files were saved to (line 617)
    - Add your Teradata username in '<enter_username_here>' (line 627)
    - Add and uncomment td_password if desired. Otherwise you will be prompted on every run (lines 628 + 629)
- Create empty directory 'output' in this file location. This is where a csv file with the output of this file will be placed
- If desired, change tables listed in ra_table_list_daily.txt if you want to run on a subset of tables

- Execute script using command: python data_validation_ra_qa.py
    - Some python packages are required and install may be needed (google-cloud-storage, tabulate)
    - You may need to specify a python version depending on your config (python3, python3.9)
    - Ensure you are in directory in which these files exist, should be same as 'basefolder' variable mentioned above

- Review results
    - CSV file written to basefolder/output/ directory
    - Results written to table in BigQuery specified on line 614. Results are appended so you will need to filter on just the most recent run to see your results 
        - Example: SELECT * FROM hca-hin-qa-cur-parallon.edwra_ac.data_validation_results WHERE DATE(sys_time) = '2024-05-14'