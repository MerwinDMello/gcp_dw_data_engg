# DAGs Information
The folder structure and associated contents within the dags folder of the repo will be reflected within the GCS bucket that syncs every time the composer workflow runs.

Folder structure and associated contents:
- config : The composer workflow will save environment configuration files here to make variables available for other scripts. DAG-specific configurations also go here.
- dataflow : Any custom code for use in Dataflow goes here
- ingest_dags : Any DAGs for data ingestion go here
- integrate_dags : Any DAGs for data integration go here
- misc_dags : Any DAGs that do not fall into the other categories go here
- outbound_dags : Any DAGs for outbound data go here
- scripts : Any modularized components of Python code for use in Airflow can be saved in this folder and called in the relevant DAG(s) using Composer's Python Operator
- sql
    - adhoc
    - ddl
        - dataset folders: DDL scripts for each dataset go in that dataset's folder
    - dml
        - ingest: DML scripts for data ingestion go here
        - integration
            - historical: Eventually, historical DML scripts will go here
            - incremental: Initially, all DML scripts for data integration will go here
        - outbound: Outbound DML scripts will go here
    - validation_sql
        - integrate
            - data source folders: SQL validation scripts for each data source go in that source's folder
- utils : Files that that go in this folder could include user-defined functions, setup files, JAR files, etc.
