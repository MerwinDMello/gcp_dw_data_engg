## Composer Environment Upgrades - Changes, Issues and Possible Resolutions
---

The new composer environment uses the Airflow 2.9.3 version. The older composer environment were built on 2.6.3 or 2.5.2 versions. 

### Changes
---

> To deploy the changes to the new composer environment, we need to change the variable in the environmental config files in the environment directory of the Git Repo to reflect the new GCS bucket. The variable is usually named as `v_dag_bucket_name`
> 
> Also, The Service Account for the new Composer environment will be different than the one we used for the previous Composer Environment. So, we need to change the `variables.tf` file in the terraform directory to use the new service account. Note that the service account will be different across environments.
> 
> The service account should be provisioned access to the necessary BigQuery Datasets in current GCP, mirroring and monitoring projects. In case of issues, do check for the provisioning or in case of views, also check how the syntax of the view has been built. 
> 
> However, while upgrading to new composer environments, you could also encounter build or execution errors for the DAGs.

### Build Errors:
---

> Newly written Dags should use **EmptyOperator** instead of **DummyOperator**.
> 
> Current DAG Code which uses the DummyOperator doesn't necessarily need to start using the EmptyOperator for the 2.9.3 Airflow. However, if you are working on any other change on the DAG Code, it would be good to replace the DummyOperator with the EmptyOperator to make it future proof.
> 
> This is the general rule based on the Airflow Version being used.
> 
> For Airflow >= 2.3.0 you should use EmptyOperator:
> ```
> from airflow.operators.empty import EmptyOperator
> ```
> For Airflow < 2.3.0 you should use DummyOperator:
> ```
> from airflow.operators.dummy import DummyOperator
> ```
> 
> Also, There were instances where the DAG Code is importing DummyOperator but using EmptyOperator. This was allowed in previous Airflow versions. However, this will fail in 2.9.3 and you should instead import and use the EmptyOperator for creating empty tasks to correct the issue and make it future proof.

### Execution Issues / Errors:
---
> The sql statement for the operator BigQueryOperator / BigQueryExecuteQueryOperator is being treated as Legacy SQL even after setting the argument use_legacy_sql as False. It needs to be replaced with BigQueryInsertJobOperator
> 
> Here is a sample replacement for reference
>
> ```
> # Code with BigQueryOperator
> BigQueryOperator(
>                 gcp_conn_id=config['env']['v_curated_conn_id'],
>                 sql = f"CALL `{config['env']['v_curated_project_id']}.{config['env'][f'v_{config_lob_suffix}_procs_dataset_name']}`.copy_table_data_with_valid_to_date_adjustment('{config['env']['v_curated_project_id']}','{v_src_db}','{v_tgt_db}','{v_table}','{v_date}', {v_update})",
>                 use_legacy_sql=False, 
>                 retries=0, 
>                 task_id = f'run_bqsql_copy_data_{v_table}'  
>             )
> ```
>
> ```
> # Code with BigQueryOperator
> BigQueryInsertJobOperator(
>             task_id = f'run_bqsql_copy_data_{v_table}' ,
>             gcp_conn_id=config['env']['v_curated_conn_id'],
>             retries=0,
>             configuration={
>                 "query": {
>                     "query": f"CALL `{config['env']['v_curated_project_id']}.{config['env'][f'v_{config_lob_suffix}_procs_dataset_name']}`.copy_table_data_with_valid_to_date_adjustment('{config['env']['v_curated_project_id']}','{v_src_db}','{v_tgt_db}','{v_table}','{v_date}', {v_update})",
>                     "useLegacySql": False,
>                 }
>             },
>         )
> ```
>
> Make sure to change the import to use [BigQueryInsertJobOperator](https://airflow.apache.org/docs/apache-airflow-providers-google/9.0.0/_api/airflow/providers/google/cloud/operators/bigquery/index.html#airflow.providers.google.cloud.operators.bigquery.BigQueryInsertJobOperator)
---
> ** Moving to BigQueryInsertJobOperator creates some additional problems which will need workarounds. **
> 2. The query argument inside the sub parameter query within configuration cannot be empty. This usually happens when elements with the sql statements are dynamically populated based on arguments in the list. If the list is empty, no SQL is generated. There was no issue with the previous BigQueryOperator but BigQueryInsertJobOperator will generate an error for empty SQL statement.
> 3. Currently, the query argument does not supports a list of sql statements. It executes the the first set of sql statements in the list and seems to be ignoring the other sets of sql statements.
> 
> Here is a sample replacement for reference for above two points.
>
> ```
>		# Old Code
> 		with TaskGroup(group_id='truncate_tables') as truncate_grp:
> 			truncate_table_list = [f"TRUNCATE TABLE `{bq_project_id}.{'.'.join([config['env'][table_name.split('.')[0]],table_name.split('.')[1]]) if len(table_name.split('.')) > 1 else '.'.join([config['env'][f'v_{lob_abbr}_stage_dataset_name'],table_name.split('.')[0]])}`;" for table_name in truncatetablelist]
> 			truncate_table = BigQueryInsertJobOperator(
> 				task_id=f"truncate_tables",
> 				gcp_conn_id=gcp_conn_id,
> 				retries=0,
> 				configuration={
> 					"query": {
> 						"query": truncate_table_list,
> 						"useLegacySql": False,
> 					}
> 				},
> 			)
> ```
>
> ```
>		# New Code
>         if truncatetablelist:
>             with TaskGroup(group_id='truncate_tables') as truncate_grp:
>                 for table_name in truncatetablelist:
>                     truncate_table_sql_statement = f"TRUNCATE TABLE `{bq_project_id}.{'.'.join([config['env'][table_name.split('.')[0]],table_name.split('.')[1]]) if len(table_name.split('.')) > 1 else '.'.join([config['env'][f'v_{lob_abbr}_stage_dataset_name'],table_name.split('.')[0]])}`;"
>                     truncate_table = BigQueryInsertJobOperator(
>                         task_id=f"truncate_table_{table_name.split('.')[1] if len(table_name.split('.')) > 1 else table_name.split('.')[0]}",
>                         gcp_conn_id=gcp_conn_id,
>                         retries=0,
>                         configuration={
>                             "query": {
>                                 "query": truncate_table_sql_statement,
>                                 "useLegacySql": False,
>                             }
>                         },
>                     )
> ```
>
> Also, Necessary changes need to be made with the dependencies to add dependency for the conditional task based on whether the list is populated.
>
---
> 4. When you check the Logging for any BigQueryInsertJobOperator task, it will generate a "Failed to send data lineage" error message. This is due to an integration issue of Composer with the GCP DataPlex service. This will not fail the Composer task necessarily and can be ignored for now.