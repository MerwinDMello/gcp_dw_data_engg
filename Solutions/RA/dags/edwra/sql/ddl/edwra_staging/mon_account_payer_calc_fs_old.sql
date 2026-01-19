-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/mon_account_payer_calc_fs_old.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_fs_old
(
  id BIGNUMERIC(38) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  mon_acct_payer_calc_summary_id BIGNUMERIC(38) NOT NULL,
  cers_term_id NUMERIC(29) NOT NULL,
  ce_service_id BIGNUMERIC(38) NOT NULL,
  service_date DATE,
  code_type STRING NOT NULL,
  code STRING NOT NULL,
  modifier1 STRING,
  modifier2 STRING,
  maximum_units NUMERIC(29),
  fee_schedule_rate NUMERIC(31, 2),
  quantity NUMERIC(29) NOT NULL,
  charge_amount NUMERIC(31, 2) NOT NULL,
  noncovered_amount NUMERIC(31, 2),
  cers_rate_amount NUMERIC(31, 2),
  cers_rate_calc_method_id NUMERIC(29) NOT NULL,
  expected_payment NUMERIC(31, 2),
  total_quantity NUMERIC(31, 2),
  total_amount NUMERIC(31, 2),
  ce_rule_id NUMERIC(29),
  max_allowed_units NUMERIC(29),
  creation_date DATETIME,
  creation_user STRING,
  modification_date DATETIME,
  modification_user STRING,
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;
