CREATE TABLE IF NOT EXISTS {{ params.param_clm_core_dataset_name }}.hin_secref_facility
(
  company_code STRING,
  user_id STRING NOT NULL,
  co_id STRING NOT NULL,
  PRIMARY KEY (co_id, user_id, company_code) NOT ENFORCED
)
CLUSTER BY co_id, user_id, company_code;