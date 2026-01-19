-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/cc_remit_amt_stg.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.cc_remit_amt_stg
(
  company_code STRING,
  coid STRING,
  patient_dw_id NUMERIC(29),
  payor_dw_id NUMERIC(29),
  remittance_advice_num STRING,
  ra_log_date DATE,
  ra_claim_payment_id BIGNUMERIC(38),
  log_id STRING,
  log_sequence_num INT64,
  unit_num STRING,
  ra_pat_acct_num NUMERIC(29),
  iplan_insurance_order_num INT64,
  ra_iplan_id INT64,
  rate_schedule_name STRING,
  financial_class STRING,
  ip_op_ind STRING,
  claim_ppscapital_amount BIGNUMERIC(40, 2),
  claim_pps_capital_outlier_amnt BIGNUMERIC(40, 2),
  pps_operat_outlier_amount BIGNUMERIC(40, 2),
  claim_hcpcs_payable_amount BIGNUMERIC(40, 2),
  claim_payment_amount BIGNUMERIC(40, 2),
  claim_drg_amount BIGNUMERIC(40, 2),
  ra835_cat_110 BIGNUMERIC(40, 2),
  ra835_cat_510 BIGNUMERIC(40, 2),
  ra835_cat_520 BIGNUMERIC(40, 2),
  ra835_cat_550 BIGNUMERIC(40, 2),
  ra835_cat_301_399 BIGNUMERIC(40, 2),
  ra835_cat_501_599 BIGNUMERIC(40, 2),
  rasrvadj_pr_code BIGNUMERIC(40, 2),
  rasrvadj_oa_code BIGNUMERIC(40, 2),
  raclaimadj_pr_code BIGNUMERIC(40, 2),
  raclaimadj_oa_code BIGNUMERIC(40, 2),
  raclaimsup_amt_i_t BIGNUMERIC(40, 2),
  raclaimsup_amt_zm BIGNUMERIC(40, 2),
  rasrv_charge_amt_cat46 BIGNUMERIC(40, 2),
  rasrv_charge_amt_cat49 BIGNUMERIC(40, 2),
  rasrv_prov_pymnt_amt_cat50 BIGNUMERIC(40, 2),
  rasrv_prov_pymnt_amt_cat88 BIGNUMERIC(40, 2),
  rasrv_prov_pymnt_amt_cat93 BIGNUMERIC(40, 2),
  dw_last_update_date_time DATETIME
)
CLUSTER BY patient_dw_id;
