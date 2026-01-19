-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/edw_job_status.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.edw_job_status
(
  etl_type STRING,
  etl_type_desc STRING,
  etl_status STRING,
  dw_last_update_date_time DATETIME
)
CLUSTER BY etl_type;
