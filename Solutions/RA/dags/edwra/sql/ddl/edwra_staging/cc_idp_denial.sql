-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/cc_idp_denial.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.cc_idp_denial
(
  coid STRING NOT NULL,
  unit_num STRING NOT NULL,
  schema_id INT64 NOT NULL,
  account_no NUMERIC(29) NOT NULL,
  iplan_id INT64 NOT NULL,
  admit_date DATE,
  discharge_date DATE,
  current_appeal_bal NUMERIC(31, 2),
  appeal_date_created DATE,
  appeal_close_date DATE,
  net_write_off_amt NUMERIC(31, 2),
  dw_last_update_date DATETIME
)
CLUSTER BY coid, unit_num, account_no, iplan_id;
