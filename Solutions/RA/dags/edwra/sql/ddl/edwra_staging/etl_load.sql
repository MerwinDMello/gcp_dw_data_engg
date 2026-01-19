-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/etl_load.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.etl_load
(
  etl_load_date DATE,
  etl_load_begin_time DATETIME,
  etl_load_end_time DATETIME,
  etl_load_reimbursement_discrepancy_date DATE,
  is_current NUMERIC(29),
  last_update_date_time DATETIME
)

CLUSTER BY etl_load_begin_time, etl_load_end_time;
