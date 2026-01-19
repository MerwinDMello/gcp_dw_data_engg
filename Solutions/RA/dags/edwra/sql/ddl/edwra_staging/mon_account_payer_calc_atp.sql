-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/mon_account_payer_calc_atp.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_atp
(
  schema_id NUMERIC(29) NOT NULL,
  id BIGNUMERIC(38) NOT NULL,
  mon_acct_payer_calc_summary_id BIGNUMERIC(38) NOT NULL,
  cers_term_id NUMERIC(29) NOT NULL,
  ce_service_id BIGNUMERIC(38) NOT NULL,
  code STRING NOT NULL,
  hcpcs_modifier_1 STRING,
  hcpcs_modifier_2 STRING,
  hcpcs_modifier_3 STRING,
  hcpcs_modifier_4 STRING,
  hcpcs_modifier_5 STRING,
  hcpcs_modifier_6 STRING,
  charge_amount NUMERIC(31, 2),
  quantity NUMERIC(29),
  service_date DATE,
  expected_payment NUMERIC(31, 2),
  pro_rate NUMERIC(31, 2),
  additional_payment NUMERIC(31, 2),
  cers_rate_calc_method_id NUMERIC(29),
  ce_rule_id NUMERIC(29),
  total_quantity NUMERIC(31, 2),
  total_amount NUMERIC(31, 2),
  atp_code STRING,
  ce_atp_profile_id NUMERIC(29),
  code_type STRING,
  individual_code_rate NUMERIC(31, 2),
  number_of_code NUMERIC(29),
  creation_date DATETIME,
  creation_user STRING,
  modification_date DATETIME,
  modification_user STRING,
  dw_last_update_date DATETIME
)
CLUSTER BY schema_id, id;
