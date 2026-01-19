CREATE TABLE IF NOT EXISTS {{ params.param_clm_core_dataset_name }}.lu_bill_type
(
  bill_type_code STRING NOT NULL,
  bill_type_desc STRING,
  dw_last_update_date_time DATETIME,
  source_system_code STRING,
  PRIMARY KEY (bill_type_code) NOT ENFORCED
)
CLUSTER BY bill_type_code;
