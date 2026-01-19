CREATE OR REPLACE VIEW `{{ params.param_monitoring_views_dataset_name }}.composer_dag_status` 
AS
SELECT
  DISTINCT domain,
  project_id,
  project_id_presentable,
  resource_type,
  user_id,
  source_system,
  load_type,
  schedule,
  dag_name,
  execution_date,
  MIN(start_ts) OVER (PARTITION BY project_id, resource_type, user_id, dag_name, execution_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS dag_start_ts,
  MAX(end_ts) OVER (PARTITION BY project_id, resource_type, user_id, dag_name, execution_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS dag_end_ts,
  LAST_VALUE(job_status) OVER (PARTITION BY project_id, resource_type, user_id, dag_name, execution_date ORDER BY task_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS dag_status,
  DATETIME_DIFF( MAX(end_ts) OVER (PARTITION BY project_id, resource_type, user_id, dag_name, execution_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), MIN(start_ts) OVER (PARTITION BY project_id, resource_type, user_id, dag_name, execution_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), SECOND) AS dag_runtime
FROM
  `{{ params.param_monitoring_views_dataset_name }}.composer_task_status`;