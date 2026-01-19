-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/cc_reconcile.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.cc_reconcile
(
  schema_id NUMERIC(29) NOT NULL,
  cc_table_name STRING NOT NULL,
  coid STRING NOT NULL,
  cc_reconcile_id NUMERIC(29) NOT NULL,
  created_date_time DATETIME NOT NULL,
  cc_reconcile_desc STRING NOT NULL,
  expected_row_cnt NUMERIC(32, 3),
  actual_row_cnt NUMERIC(32, 3),
  updated_date_time DATETIME
)
PARTITION BY DATE_TRUNC(created_date_time, MONTH)
CLUSTER BY schema_id, cc_table_name, coid, cc_reconcile_id;
