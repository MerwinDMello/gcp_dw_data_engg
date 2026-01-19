# DAGs Information
The folder structure and associated contents within the dags folder of the repo will be reflected within the GCS bucket that syncs every time the composer workflow runs.

Folder structure and associated contents:
- composer
    - ingest : Any DAGs for data ingestion go here
    - integrate : Any DAGs for data integration go here
    - misc : Any DAGs that do not fall into the other categories go here
    - outbound : Any DAGs for outbound data go here
- config : The composer workflow will save environment configuration files here to make environment-based variables available for other scripts. DAG-specific configurations also go here.
- sql
    - ingest: DML scripts for data ingestion go here
    - integrate: ML scripts for data integration go here

# A Note About File Path References in Scripts
The code below is the recommended syntax to use to reference file paths in your scripts to establish the path based on the location of the script itself

```python
script_dir = os.path.dirname(__file__) # set script_dir to the directory location of the current file
util_dir = os.path.join(script_dir, '..','utils') # In this example, the utils folder is found by going up a level from the location of the current file
sys.path.append(util_dir)
```
