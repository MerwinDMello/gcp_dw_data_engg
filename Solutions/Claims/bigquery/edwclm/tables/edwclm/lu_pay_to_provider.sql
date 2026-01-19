CREATE TABLE IF NOT EXISTS {{ params.param_clm_core_dataset_name }}.lu_pay_to_provider
(
  pay_to_provider_sid NUMERIC(29) NOT NULL,
  pay_to_provider_name STRING,
  pay_to_provider_addr1 STRING,
  pay_to_provider_addr2 STRING,
  pay_to_provider_city STRING,
  pay_to_provider_st STRING,
  pay_to_provider_zip_cd STRING,
  dw_last_update_date_time DATETIME,
  source_system_code STRING,
  PRIMARY KEY (pay_to_provider_sid) NOT ENFORCED
)
CLUSTER BY pay_to_provider_sid;
