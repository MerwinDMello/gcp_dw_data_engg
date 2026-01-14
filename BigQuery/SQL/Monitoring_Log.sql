SELECT
labels.workflow AS dag_name,
labels.task_id AS task_name,
SAFE_CAST(SUBSTR(labels.execution_date,1,19) AS TIMESTAMP) AS execution_date,
timestamp AS log_timestamp,
textPayload
FROM
hca-hin-monitoring-hr.bq_sink_d6b788_log.airflow_worker_20*
WHERE
labels.workflow NOT LIKE 'airflow_monitoring%'
AND DATE(timestamp, "US/Central") = '2023-09-07'
AND resource.labels.project_id = 'hca-hin-prod-proc-hr'
AND textPayload LIKE '%Marking task as FAILED%'
Order By dag_name, task_name, log_timestamp