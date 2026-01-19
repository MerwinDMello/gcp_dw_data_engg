CREATE TABLE IF NOT EXISTS {{ params.param_rmt_stage_dataset_name }}.lu_remit_payer
(
  remit_payer_id STRING NOT NULL,
  payer_name STRING,
  address_1 STRING,
  address_2 STRING,
  city STRING,
  state STRING,
  zip STRING,
  country_cd STRING,
  payer_id_num STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME,
  customer_cd STRING
)
CLUSTER BY remit_payer_id;
