CREATE TABLE IF NOT EXISTS {{ params.param_clm_core_dataset_name }}.ref_claim_payor
(
  custom_id STRING,
  claim_type_code STRING,
  claim_payor_id INT64,
  claim_type_desc STRING,
  payor_name STRING,
  source_pay_code STRING,
  source_pay_desc STRING,
  dw_last_update_date_time DATETIME,
  source_system_code STRING,
  PRIMARY KEY (custom_id, claim_type_code, claim_payor_id) NOT ENFORCED
)
CLUSTER BY custom_id, claim_type_code, claim_payor_id;
