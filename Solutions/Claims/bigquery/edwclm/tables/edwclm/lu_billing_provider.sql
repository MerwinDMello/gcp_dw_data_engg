CREATE TABLE IF NOT EXISTS {{ params.param_clm_core_dataset_name }}.lu_billing_provider
(
  bill_provider_sid NUMERIC(29) NOT NULL,
  bill_provider_name STRING,
  bill_provider_addr1 STRING,
  bill_provider_addr2 STRING,
  bill_provider_city STRING,
  bill_provider_st STRING,
  bill_provider_zip_cd STRING,
  bill_provider_npi NUMERIC(29),
  dw_last_update_date_time DATETIME,
  source_system_code STRING,
  PRIMARY KEY (bill_provider_sid) NOT ENFORCED
)
CLUSTER BY bill_provider_sid;
