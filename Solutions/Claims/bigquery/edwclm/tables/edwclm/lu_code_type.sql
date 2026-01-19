CREATE TABLE IF NOT EXISTS {{ params.param_clm_core_dataset_name }}.lu_code_type
(
  code_type_id STRING NOT NULL,
  code_type_desc STRING,
  dw_last_update_date_time DATETIME,
  source_system_code STRING,
  PRIMARY KEY (code_type_id) NOT ENFORCED
)
CLUSTER BY code_type_id;
