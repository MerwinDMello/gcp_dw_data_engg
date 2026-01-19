CREATE TABLE IF NOT EXISTS {{ params.param_clm_stage_dataset_name }}.fact_claim_insurance
(
  claim_id STRING NOT NULL,
  payor_seq_ind STRING NOT NULL,
  iplan_id STRING,
  payer_id STRING,
  payer_sub_id STRING,
  payer_name STRING,
  health_plan_id STRING,
  release_info_cert_desc STRING,
  assign_benefit_cert_desc STRING,
  prior_pay_amt NUMERIC(32, 3),
  est_due_amt NUMERIC(32, 3),
  other_provider_id STRING,
  insured_name STRING,
  pat_to_ins_rel_ind STRING,
  insured_id STRING,
  insured_group_name STRING,
  insured_group_num STRING,
  treatment_auth_code STRING,
  doc_cntrl_num STRING,
  employer_name STRING,
  dw_last_update_date_time DATETIME,
  source_system_code STRING
)
CLUSTER BY claim_id, payor_seq_ind;
