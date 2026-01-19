CREATE TABLE IF NOT EXISTS {{ params.param_clm_stage_dataset_name }}.fact_claim_patient
(
  claim_id STRING NOT NULL,
  patient_last_name STRING,
  patient_first_name STRING,
  patient_addr1 STRING,
  patient_addr2 STRING,
  patient_city STRING,
  patient_st STRING,
  patient_zip_cd STRING,
  patient_sex_cd STRING,
  patient_dob DATE,
  resp_party_name STRING,
  resp_party_addr1 STRING,
  resp_party_addr2 STRING,
  resp_party_city STRING,
  resp_party_st STRING,
  resp_party_zip_cd STRING,
  dw_last_update_date_time DATETIME,
  source_system_code STRING
)
CLUSTER BY claim_id;
