-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/mon_account_payer_calc_coin_fs.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_coin_fs
(
  id BIGNUMERIC(38) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  cers_term_id NUMERIC(29) NOT NULL,
  mon_acct_payer_calc_summary_id BIGNUMERIC(38) NOT NULL,
  ce_service_id BIGNUMERIC(38) NOT NULL,
  service_date DATE NOT NULL,
  code STRING NOT NULL,
  code_type STRING NOT NULL,
  modifier_1 STRING,
  modifier_2 STRING,
  max_units NUMERIC(29),
  quantity NUMERIC(29) NOT NULL,
  charge_amount NUMERIC(31, 2) NOT NULL,
  fee_schedule_rate NUMERIC(32, 3),
  cers_rate_amount NUMERIC(31, 2),
  cers_rate_calc_method_id NUMERIC(29) NOT NULL,
  expected_payment NUMERIC(31, 2),
  total_quantity NUMERIC(29),
  total_amount NUMERIC(31, 2),
  ce_rule_id NUMERIC(29),
  coins_calc_method_id NUMERIC(29),
  coins_value NUMERIC(31, 2),
  coins_amount NUMERIC(31, 2),
  calculated_coins_payment NUMERIC(31, 2),
  ce_coins_fs_category_id NUMERIC(29),
  priority NUMERIC(29),
  exclude_from_coins_ded_calc NUMERIC(29),
  deductible_amount NUMERIC(31, 2),
  is_calc_coins_amount_included NUMERIC(29),
  op_deductible NUMERIC(31, 2),
  creation_date DATETIME,
  creation_user STRING,
  modification_date DATETIME,
  modification_user STRING,
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;
