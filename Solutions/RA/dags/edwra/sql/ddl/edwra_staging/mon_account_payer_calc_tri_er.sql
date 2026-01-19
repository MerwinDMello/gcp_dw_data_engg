-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/mon_account_payer_calc_tri_er.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_tri_er
(
  id BIGNUMERIC(38) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  mon_acct_payer_calc_summary_id BIGNUMERIC(38) NOT NULL,
  cers_term_id NUMERIC(29) NOT NULL,
  ce_service_id BIGNUMERIC(38) NOT NULL,
  ce_rule_id NUMERIC(29) NOT NULL,
  cers_rate_calc_method_id NUMERIC(29) NOT NULL,
  hcpc_code STRING NOT NULL,
  hcpc_code_modifier_1 STRING,
  hcpc_code_modifier_2 STRING,
  hcpc_code_modifier_3 STRING,
  hcpc_code_modifier_4 STRING,
  revenue_code STRING NOT NULL,
  amount NUMERIC(31, 2) NOT NULL,
  quantity NUMERIC(29) NOT NULL,
  service_date DATE NOT NULL,
  service_date_begin DATE,
  service_date_end DATE,
  expected_payment NUMERIC(31, 2),
  total_quantity NUMERIC(31, 2) NOT NULL,
  total_amount NUMERIC(31, 2) NOT NULL,
  noncovered_amount NUMERIC(31, 2),
  ce_tricare_emer_rm_profile_id NUMERIC(29) NOT NULL,
  ce_tri_emer_rm_hcpc_code_id NUMERIC(29) NOT NULL,
  unadjustedpayment_rate NUMERIC(31, 2) NOT NULL,
  er_wage_index NUMERIC(37, 8),
  er_labor_portion NUMERIC(37, 8),
  er_cost_share_percent NUMERIC(37, 8),
  er_cost_share_amount NUMERIC(31, 2),
  date_created DATE NOT NULL,
  creation_date DATETIME,
  creation_user STRING,
  modification_date DATETIME,
  modification_user STRING,
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;
