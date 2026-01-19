CREATE TABLE IF NOT EXISTS {{ params.param_clm_core_dataset_name }}.lu_physician_qualifier
(
  phys_qual_code STRING NOT NULL,
  phys_qual_837_code STRING,
  phys_qual_desc STRING,
  dw_last_update_date_time DATETIME,
  source_system_code STRING,
  PRIMARY KEY (phys_qual_code) NOT ENFORCED
)
CLUSTER BY phys_qual_code;
