-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/mon_account_payer_calc_snf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_snf
(
  id BIGNUMERIC(38) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  mon_acct_payer_calc_summary_id BIGNUMERIC(38) NOT NULL,
  cers_term_id NUMERIC(29) NOT NULL,
  ce_snf_profile_id NUMERIC(29) NOT NULL,
  ce_service_id BIGNUMERIC(38) NOT NULL,
  charge_amount NUMERIC(31, 2),
  expected_payment NUMERIC(31, 2),
  hipps_code STRING,
  adj_labor_rate NUMERIC(31, 2),
  adj_rate NUMERIC(31, 2),
  icd9_042_multiplier NUMERIC(33, 4),
  service_date_from DATE,
  service_date_to DATE,
  icd9_042_present NUMERIC(29),
  wage_index NUMERIC(34, 5),
  quantity NUMERIC(29),
  total_quantity NUMERIC(31, 2),
  total_amount NUMERIC(31, 2),
  ce_rule_id NUMERIC(29),
  snf_cbsa_wage_index NUMERIC(34, 5),
  total_rate NUMERIC(31, 2),
  labor_portion NUMERIC(31, 2),
  non_labor_portion NUMERIC(31, 2),
  creation_date DATETIME,
  creation_user STRING,
  modification_date DATETIME,
  modification_user STRING,
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;
