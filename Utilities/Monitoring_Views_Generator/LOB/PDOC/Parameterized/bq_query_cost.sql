CREATE OR REPLACE VIEW `{{ params.param_monitoring_views_dataset_name }}.bq_query_cost`
AS
SELECT
  COALESCE(completed.domain, running.domain) AS domain,
  COALESCE(completed.project_id, running.project_id ) AS project_id,
  CONCAT (
    CASE TRIM(LOWER(SPLIT(COALESCE(completed.project_id, running.project_id ),'-')[SAFE_OFFSET (3)]))
      WHEN 'landing' THEN 'Landing'
      WHEN 'cur' THEN 'Curated'
      WHEN 'proc' THEN 'Processing'
  END
    , ' - ', UPPER(TRIM(SPLIT(COALESCE(completed.project_id, running.project_id ),'-')[SAFE_OFFSET (4)])), ' ',
    CASE TRIM(LOWER(SPLIT(COALESCE(completed.project_id, running.project_id ),'-')[SAFE_OFFSET (2)]))
      WHEN 'qa' THEN 'QA'
      WHEN 'prod' THEN 'Prod'
      WHEN 'dev' THEN 'Dev'
  END
    ) AS project_id_presentable,
  COALESCE(completed.location, running.location ) AS location,
  CASE
    WHEN TRIM(LOWER(COALESCE(completed.location, running.location))) = 'us' THEN 'US'
    WHEN TRIM(LOWER(COALESCE(completed.location, running.location))) = 'global' THEN 'Global'
    WHEN STARTS_WITH(TRIM(LOWER(COALESCE(completed.location, running.location))), 'us-') 
    THEN CONCAT('US - ',INITCAP(REGEXP_EXTRACT(COALESCE(completed.location, running.location), r'^[a-zA-Z]+-([a-zA-Z]+).*$')))
  ELSE
  'No location specified'
END
  AS location_presentable,
  COALESCE(completed.user, running.user ) AS user,
IF
  (ENDS_WITH(COALESCE(completed.user, running.user ), 'gserviceaccount.com'),'Service Account','User') AS user_type,
IF (ENDS_WITH(COALESCE(completed.user, running.user), 'gserviceaccount.com'),
    CONCAT(
    CASE
    REGEXP_EXTRACT(TRIM(LOWER(SPLIT(COALESCE(completed.user, running.user), '@')[SAFE_OFFSET (0)])), 
    r'(?:streaming|td-.*-migration|composer|dvt|bq-mig-landing|bq-mig|pwrbi|df-atos|tel-poc|dbt|td-.*-migrate|-td|-tf|qlik|lkr|looker|mstr|bobj|irc)')
    WHEN 'streaming' THEN 'Streaming'
    WHEN 'composer' THEN 'Composer'
    WHEN 'df-atos' THEN 'Dataflow'
    WHEN 'bq-mig-landing' THEN 'BQ Migration Landing'
    WHEN 'bq-mig' THEN 'BQ Migration'
    WHEN 'pwrbi' THEN 'Power BI'
    WHEN 'qlik' THEN 'Qlikview'
    WHEN 'lkr' THEN 'Looker'
    WHEN 'looker' THEN 'Looker'
    WHEN 'mstr' THEN 'Microstrategy'
    WHEN 'bobj' THEN 'BobJ'
    WHEN 'td-clinical-migration' THEN 'TD Migration'
    WHEN 'td-clinical-migrate' THEN 'TD Migration'
    WHEN '-td' THEN 'TD Migration'
    WHEN 'dvt' THEN 'DVT'
    WHEN 'dbt' THEN 'DBT'
    WHEN 'irc' THEN 'IRC'
    WHEN 'tel-poc' THEN 'TEL POC'
    WHEN '-tf' THEN 'Terraform'
    ELSE ''
    END,
    ' - ',
    CONCAT(
      CASE TRIM(LOWER(SPLIT(SPLIT(COALESCE(completed.user, running.user), '@')[SAFE_OFFSET (1)],'.')[SAFE_OFFSET (0)]))
      WHEN 'hca-at-cao-sa-mgmt' 
        THEN 
          CASE REGEXP_EXTRACT(TRIM(LOWER(SPLIT(COALESCE(completed.user, running.user), '@')[SAFE_OFFSET (0)])), r'.*-(clin)(?:-.*|$)')
            WHEN 'clin' THEN 'Clinical'
            ELSE ''
          END
      ELSE
        CASE REGEXP_EXTRACT(TRIM(LOWER(SPLIT(SPLIT(COALESCE(completed.user, running.user), '@')[SAFE_OFFSET (1)],'.')[SAFE_OFFSET (0)])), r'.*-(clin)[-]*.*')
            WHEN 'clin' THEN 'Clinical'
          ELSE ''
        END
      END,
      ' ',
      CASE TRIM(LOWER(SPLIT(SPLIT(COALESCE(completed.user, running.user), '@')[SAFE_OFFSET (1)],'.')[SAFE_OFFSET (0)]))
      WHEN 'hca-at-cao-sa-mgmt' 
        THEN 
          CASE REGEXP_EXTRACT(TRIM(LOWER(SPLIT(COALESCE(completed.user, running.user), '@')[SAFE_OFFSET (0)])), r'.*-(qa|dev|prod)(?:-.*|$)')
            WHEN 'prod' THEN 'Prod'
            WHEN 'qa' THEN 'QA'
            WHEN 'dev' THEN 'Dev'
            ELSE ''
          END
      ELSE
        CASE REGEXP_EXTRACT(TRIM(LOWER(SPLIT(SPLIT(COALESCE(completed.user, running.user), '@')[SAFE_OFFSET (1)],'.')[SAFE_OFFSET (0)])), r'.*-(qa|dev|prod)[-]*.*')
          WHEN 'prod' THEN 'Prod'
          WHEN 'qa' THEN 'QA'
          WHEN 'dev' THEN 'Dev'
          ELSE ''
        END
      END
    )
  ),
  COALESCE(completed.user, running.user)
) AS user_presentable,
  CASE TRIM(LOWER(SPLIT(COALESCE(completed.project_id, running.project_id ),'-')[SAFE_OFFSET (2)]))
      WHEN 'qa' THEN 'QA'
      WHEN 'prod' THEN 'Prod'
      WHEN 'dev' THEN 'Dev'
  END AS environment,
  COALESCE(completed.job_name, running.job_name ) AS job_name,
  COALESCE(completed.ip_address, running.ip_address ) AS ip_address,
  COALESCE(completed.user_agent, running.user_agent ) AS user_agent,
  -- TIMESTAMPS --
  running.timestamp AS rts,
  completed.timestamp AS cts,
  SAFE_CAST(SUBSTR(coalesce(completed.start_time, running.start_time),1,19) AS TIMESTAMP) AS start_time,
  SAFE_CAST(SUBSTR(coalesce(completed.job_end_time, running.job_end_time),1,19) AS TIMESTAMP) AS end_time,
  -- END TIMESTAMPS --
  COALESCE(completed.query, running.query ) AS query,
  -- BILLING INFORMATION --
  CAST( COALESCE(completed.billingTier, running.billingTier ) AS int64) AS billingTier,
  CAST( COALESCE(completed.totalBilledBytes, running.totalBilledBytes ) AS int64) AS totalBilledBytes,
  (CAST( COALESCE(completed.totalBilledBytes, running.totalBilledBytes ) AS int64)/1000000000000.0) * 5.0 AS costUSD,
  CAST(COALESCE(completed.totalProcessedBytes, running.totalProcessedBytes) AS int64) AS totalProcessedBytes,
  CAST( COALESCE(completed.totalSlotMs, running.totalSlotMs ) AS int64) AS totalSlotMs,
  (CAST(COALESCE(completed.totalSlotMs, running.totalSlotMs ) AS int64)/(2000*1000.0)) AS projectSlotSeconds,
  -- END BILLING INFORMATION --
  COALESCE(completed.status, running.status ) AS status,
  COALESCE(completed.msg, running.msg ) AS msg,
  running.metadata AS rm,
  completed.metadata AS cm
FROM (
  SELECT
    'Clinical' AS domain,
    A.resource.labels.project_id AS project_id,
    a.resource.labels.location AS location,
    LOWER(TRIM(a.protopayload_auditlog.authenticationinfo.principalemail)) AS user,
    a.protopayload_auditlog.resourcename AS job_name,
    a.protopayload_auditlog.requestmetadata.callerip AS ip_address,
    a.protopayload_auditlog.requestmetadata.callersupplieduseragent AS user_agent,
    JSON_EXTRACT_SCALAR(a.protopayload_auditlog.metadatajson, "$.jobInsertion.job.jobConfig.queryConfig.query" ) AS query,
    timestamp AS timestamp,
    JSON_EXTRACT_SCALAR(a.protopayload_auditlog.metadatajson, "$.jobInsertion.job.jobStats.startTime" ) AS start_time,
    CAST(NULL AS STRING) AS job_end_time,
    'Running' AS Status,
    a.protopayload_auditlog.status.message AS msg,
    JSON_EXTRACT_SCALAR(a.protopayload_auditlog.metadataJson, "$.jobChange.job.jobStats.queryStats.billingTier" ) AS billingTier,
    JSON_EXTRACT_SCALAR(a.protopayload_auditlog.metadataJson, "$.jobChange.job.jobStats.queryStats.totalBilledBytes" ) AS totalBilledBytes,
    JSON_EXTRACT_SCALAR(a.protopayload_auditlog.metadataJson, "$.jobChange.job.jobStats.queryStats.totalProcessedBytes") AS totalProcessedBytes,
    JSON_EXTRACT_SCALAR(a.protopayload_auditlog.metadataJson, "$.jobChange.job.jobStats.totalSlotMs" ) AS totalSlotMs,
    a.insertid AS insertid,
    a.operation.id AS operation_id,
    a.protopayload_auditlog.metadatajson AS metadata
  FROM
    `{{ params.param_auth_base_views_dataset_name }}.cloudaudit_googleapis_com_data_access` a
  WHERE
    1=1
    AND a.protopayload_auditlog.methodname = 'google.cloud.bigquery.v2.JobService.InsertJob'
    AND a.operation.first IS TRUE ) running
FULL OUTER JOIN (
  SELECT
    'Clinical' AS domain,
    A.resource.labels.project_id AS project_id,
    a.resource.labels.location AS location,
    LOWER(TRIM(a.protopayload_auditlog.authenticationinfo.principalemail)) AS user,
    a.protopayload_auditlog.resourcename AS job_name,
    a.protopayload_auditlog.requestmetadata.callerip AS ip_address,
    a.protopayload_auditlog.requestmetadata.callersupplieduseragent AS user_agent,
    JSON_EXTRACT_SCALAR(a.protopayload_auditlog.metadatajson, "$.jobInsertion.job.jobConfig.queryConfig.query" ) AS query,
    timestamp AS timestamp,
    JSON_EXTRACT_SCALAR(a.protopayload_auditlog.metadatajson, "$.jobInsertion.job.jobStats.startTime" ) AS start_time,
    JSON_EXTRACT_SCALAR(a.protopayload_auditlog.metadataJson, "$.jobChange.job.jobStats.endTime" ) AS job_end_time,
    'Completed' AS Status,
    a.protopayload_auditlog.status.message AS msg,
    JSON_EXTRACT_SCALAR(a.protopayload_auditlog.metadataJson, "$.jobChange.job.jobStats.queryStats.billingTier" ) AS billingTier,
    JSON_EXTRACT_SCALAR(a.protopayload_auditlog.metadataJson, "$.jobChange.job.jobStats.queryStats.totalBilledBytes" ) AS totalBilledBytes,
    JSON_EXTRACT_SCALAR(a.protopayload_auditlog.metadataJson, "$.jobChange.job.jobStats.queryStats.totalProcessedBytes") AS totalProcessedBytes,
    JSON_EXTRACT_SCALAR(a.protopayload_auditlog.metadataJson, "$.jobChange.job.jobStats.totalSlotMs" ) AS totalSlotMs,
    a.insertid AS insertid,
    a.operation.id AS operation_id,
    a.protopayload_auditlog.metadatajson AS metadata
  FROM
    `{{ params.param_auth_base_views_dataset_name }}.cloudaudit_googleapis_com_data_access` a
  WHERE
    1=1
    AND a.protopayload_auditlog.methodname = 'google.cloud.bigquery.v2.JobService.InsertJob'
    AND a.operation.last IS TRUE ) completed
ON
  running.operation_id = completed.operation_id
  AND COALESCE(completed.start_time, running.start_time) IS NOT NULL