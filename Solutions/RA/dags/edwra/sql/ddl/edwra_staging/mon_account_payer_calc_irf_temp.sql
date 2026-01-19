-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/mon_account_payer_calc_irf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_irf_temp
(
  id BIGNUMERIC(38) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  mon_acct_payer_calc_summary_id BIGNUMERIC(38) NOT NULL,
  cers_term_id NUMERIC(29) NOT NULL,
  ce_irf_profile_id NUMERIC(29),
  ce_service_id BIGNUMERIC(38) NOT NULL,
  charge_amount NUMERIC(31, 2),
  expected_payment NUMERIC(31, 2),
  hipps_code STRING,
  base_rate NUMERIC(31, 2),
  wage_adj_rate NUMERIC(31, 2),
  wage_rural_adj_rate NUMERIC(31, 2),
  wage_rural_lip_adj_rate NUMERIC(31, 2),
  total_adj_rate NUMERIC(31, 2),
  outlier_threshold NUMERIC(31, 2),
  medicare_allowable NUMERIC(31, 2),
  service_date_from DATE,
  service_date_to DATE,
  outlier_payment NUMERIC(31, 2),
  quantity NUMERIC(29),
  total_quantity NUMERIC(31, 2),
  total_amount NUMERIC(31, 2),
  ce_rule_id NUMERIC(29),
  irf_std_payment_conv_factor NUMERIC(31, 2),
  irf_lip_adjustment NUMERIC(35, 6),
  irf_non_labor_amount NUMERIC(31, 2),
  irf_comorbid_tier_multiplier NUMERIC(33, 4),
  irf_teaching_status_adjustment NUMERIC(31, 2),
  irf_transfer_shortstay_amount NUMERIC(31, 2),
  is_irf_transfer_calc NUMERIC(33, 4),
  wage_xfer_rural_lip_adj_rate NUMERIC(31, 2),
  irf_xfer_teaching_status_adj NUMERIC(31, 2),
  alos NUMERIC(29),
  adjusted_outlier_threshold NUMERIC(31, 2),
  creation_date DATETIME,
  creation_user STRING,
  modification_date DATETIME,
  modification_user STRING,
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;
