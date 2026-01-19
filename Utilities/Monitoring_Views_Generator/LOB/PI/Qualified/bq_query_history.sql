CREATE OR REPLACE VIEW `hca-hin-dev-cur-ops.edwops_ac.bq_query_history`
AS
SELECT
  COALESCE(job_complete.domain, job_start.domain) AS domain,
  COALESCE(job_complete.project_id,job_start.other_project_id ) AS job_project_id,
  COALESCE(job_start.project_id,job_complete.project_id) AS data_project_id,
  CASE
    WHEN COALESCE(job_complete.project_id,job_start.other_project_id )!=COALESCE(job_start.project_id,job_complete.project_id) THEN 1
  ELSE
  0
END
  AS different_project_ind,
  COALESCE(job_complete.resource_type,job_start.resource_type) AS resource_type,
  COALESCE(job_complete.location,job_start.location) AS location,
  CASE
    WHEN TRIM(LOWER(COALESCE(job_complete.location,job_start.location))) = 'us' THEN 'US'    
    WHEN TRIM(LOWER(COALESCE(job_complete.location,job_start.location))) = 'global' THEN 'Global'
    WHEN STARTS_WITH(TRIM(LOWER(COALESCE(job_complete.location,job_start.location))), 'us-') 
    THEN CONCAT('US - ',INITCAP(REGEXP_EXTRACT(COALESCE(job_complete.location,job_start.location), r'^[a-zA-Z]+-([a-zA-Z]+).*$')))
  ELSE
  'No location specified'
END
  AS location_presentable,
  COALESCE(job_complete.user_id,job_start.user_id) AS user_id,
  COALESCE(job_complete.job_id,job_start.job_id) AS job_id,
  COALESCE(job_complete.start_ts,job_start.start_ts) AS start_ts,
  job_complete.end_ts end_ts,
  COALESCE(job_complete.ip, job_start.ip) AS ip,
  COALESCE(job_complete.user_agent,job_start.user_agent) AS user_agent,
  COALESCE(job_complete.query,job_start.query) AS query,
  COALESCE(job_complete.status_code,job_start.status_code) AS status_code,
  COALESCE(job_complete.status_message, job_start.status_message) AS status_message,
  job_complete.destination_project,
  job_complete.destination_dataset,
  job_complete.destination_table,
  job_complete.statement_type,
  job_complete.write_disposition,
  job_complete.processed_bytes,
  job_complete.totalSlotMs
FROM (
  SELECT
    'OPS' AS domain,
    resource.labels.project_id AS project_id,
    resource.type AS resource_type,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.location AS location,
    protopayload_auditlog.authenticationInfo.principalEmail AS user_id,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId AS job_id,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime AS start_ts,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.endTime AS end_ts,
    protopayload_auditlog.requestMetadata.callerIp AS ip,
    protopayload_auditlog.requestMetadata.callerSuppliedUserAgent user_agent,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.query,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.destinationTable.projectId destination_project,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.destinationTable.datasetId destination_dataset,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.destinationTable.tableId destination_table,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.statementType statement_type,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.writeDisposition write_disposition,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatus.error.code status_code,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatus.error.message status_message,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalProcessedBytes processed_bytes,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalSlotMs
  FROM
    `hca-hin-dev-cur-ops.auth_base_views.cloudaudit_googleapis_com_data_access`
  WHERE
    resource.type = 'bigquery_resource'
    AND protopayload_auditlog.methodName = 'jobservice.jobcompleted' ) job_complete
FULL OUTER JOIN (
  SELECT
    'OPS' AS domain,
    resource.labels.project_id AS project_id,
    protopayload_auditlog.servicedata_v1_bigquery.jobInsertResponse.resource.jobName.projectId other_project_id,
    resource.type AS resource_type,
    protopayload_auditlog.servicedata_v1_bigquery.jobInsertRequest.resource.jobName.location AS location,
    protopayload_auditlog.authenticationInfo.principalEmail AS user_id,
    COALESCE(coalesce(protopayload_auditlog.servicedata_v1_bigquery.jobInsertRequest.resource.jobName.jobId,
        protopayload_auditlog.servicedata_v1_bigquery.jobInsertResponse.resource.jobName.jobId),CONCAT('log_insertId=',insertId)) AS job_id,
    COALESCE(protopayload_auditlog.servicedata_v1_bigquery.jobInsertResponse.resource.jobStatistics.startTime, timestamp) AS start_ts,
    CAST(NULL AS timestamp) AS end_ts,
    protopayload_auditlog.status.code AS status_code,
    protopayload_auditlog.status.message AS status_message,
    protopayload_auditlog.requestMetadata.callerIp AS ip,
    protopayload_auditlog.requestMetadata.callerSuppliedUserAgent user_agent,
    protopayload_auditlog.servicedata_v1_bigquery.jobInsertRequest.resource.jobConfiguration.query.query AS query
  FROM
    `hca-hin-dev-cur-ops.auth_base_views.cloudaudit_googleapis_com_data_access`
  WHERE
    resource.type = 'bigquery_resource'
    AND protopayload_auditlog.methodName = 'jobservice.insert'
    AND NOT COALESCE(protopayload_auditlog.servicedata_v1_bigquery.jobInsertResponse.resource.jobConfiguration .dryRun, FALSE)=TRUE
    AND protopayload_auditlog.servicedata_v1_bigquery.jobInsertResponse.resource.jobName.projectId!='google.com:bigquery-billing-export' ) job_start
ON
  job_complete.job_id = job_start.job_id;