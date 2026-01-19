# BigQuery Folder Information
It is important to place scripts within the relevant folder as noted below in order for proper deployment to occur. 

This repository should ONLY be used to deploy staging tables. All other DDLs should be stored in and deployed by the domain's centralized DDL repo.

BigQuery DDL deployment folder structure:
- bigquery
    - subdomain folder
        - tables: scripts for tables
        - udf: scripts for user defined functions
        - routines: scripts for stored procedures
            - adhoc: all adhoc/hotfix scripts should go here
              (It is important to follow this structure so that in the event of needing to rerun all DDL scripts the adhoc scripts will NOT be run)

# A Note About File Path References in Scripts
The code below is the recommended syntax to use to reference file paths in your scripts to establish the path based on the location of the script itself

```python
script_dir = os.path.dirname(__file__) # set script_dir to the directory location of the current file
util_dir = os.path.join(script_dir, '..','config') # In this example, the config folder is found by going up a level from the location of the current file
sys.path.append(util_dir)
```
