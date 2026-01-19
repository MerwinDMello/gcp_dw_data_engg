CREATE TABLE IF NOT EXISTS {{ params.param_clm_core_dataset_name }}.lu_physician_type
(
  phys_type_code STRING NOT NULL,
  phys_type_qual_nm101_code STRING,
  phys_type_desc STRING,
  dw_last_update_date_time DATETIME,
  source_system_code STRING,
  PRIMARY KEY (phys_type_code) NOT ENFORCED
)
CLUSTER BY phys_type_code;
