CREATE TABLE IF NOT EXISTS {{ params.param_rmt_core_dataset_name }}.lu_remit_provider
(
  remit_provider_id STRING NOT NULL,
  provider_name STRING,
  address_1 STRING,
  address_2 STRING,
  city STRING,
  state STRING,
  zip STRING,
  country_cd STRING,
  npi STRING,
  tax_id STRING,
  dw_last_update_date_time DATETIME,
  source_system_code STRING,
  customer_cd STRING,
  PRIMARY KEY (customer_cd, remit_provider_id) NOT ENFORCED
)
CLUSTER BY customer_cd, remit_provider_id;
