-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/crt_open_gov_overpayment.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.crt_open_gov_overpayment_temp
(
  log_id INT64 NOT NULL,
  unit_num INT64,
  coid STRING,
  request_type_desc STRING,
  request_date DATETIME,
  account_no STRING,
  fin_class STRING,
  last_activity_date DATETIME,
  status_desc STRING,
  disc_date DATETIME,
  discsourcedesc STRING,
  reimbursementimpact STRING,
  reprreasons STRING,
  queuename STRING,
  extract_date DATETIME
)
CLUSTER BY log_id;
