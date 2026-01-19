-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/reimbursement_stats.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.reimbursement_stats
(
  coid STRING,
  overpayment NUMERIC(32, 3),
  underpayment NUMERIC(32, 3),
  non_financial NUMERIC(32, 3),
  overpayment_count INT64,
  underpayment_count INT64,
  non_financial_count INT64,
  total_discrepancies INT64,
  dw_last_update_date_time DATETIME
)
CLUSTER BY coid;
