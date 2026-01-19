CREATE TABLE IF NOT EXISTS {{ params.param_clm_core_dataset_name }}.fact_charge
(
  claim_id STRING NOT NULL,
  charge_seq_num INT64 NOT NULL,
  charge_revenue_code STRING,
  charge_ndc_code STRING,
  charge_hcpcs STRING,
  charge_rate_value STRING,
  charge_hcpcs_modifier1_cd STRING,
  charge_hcpcs_modifier2_cd STRING,
  charge_hcpcs_modifier3_cd STRING,
  charge_hcpcs_modifier4_cd STRING,
  charge_service_dt DATE,
  charge_unit_of_svc_num NUMERIC(32, 3),
  charge_total_amt NUMERIC(32, 3),
  charge_non_covered_amt NUMERIC(32, 3),
  dw_last_update_date_time DATETIME,
  source_system_code STRING,
  charge_ndc_drug_qty NUMERIC(32, 3),
  charge_ndc_drug_uom STRING,
  PRIMARY KEY (claim_id, charge_seq_num) NOT ENFORCED
)
CLUSTER BY claim_id, charge_seq_num;
