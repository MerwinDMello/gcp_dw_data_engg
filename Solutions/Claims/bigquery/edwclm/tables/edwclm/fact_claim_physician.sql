CREATE TABLE IF NOT EXISTS {{ params.param_clm_core_dataset_name }}.fact_claim_physician
(
  claim_id STRING NOT NULL,
  phys_type_code STRING NOT NULL,
  phys_qual_code STRING NOT NULL,
  phys_code STRING,
  phys_last_name STRING,
  phys_first_name STRING,
  phys_taxonomy_code STRING,
  dw_last_update_date_time DATETIME,
  source_system_code STRING,
  PRIMARY KEY (claim_id, phys_type_code, phys_qual_code) NOT ENFORCED
)
CLUSTER BY claim_id, phys_type_code, phys_qual_code;
