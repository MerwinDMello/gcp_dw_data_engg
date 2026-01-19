create table if not exists `{{ params.param_hr_stage_dataset_name }}.zsspancode`
(
  hca_span_code STRING,
  hca_syscode STRING,
  company INT64,
  process_level STRING,
  hca_unit INT64,
  hca_coid INT64,
  name_sp STRING,
  hca_group INT64,
  hca_division INT64,
  hca_market INT64,
  hca_flevel INT64,
  hca_lob STRING,
  hca_sub_lob STRING,
  hca_uid_id STRING,
  date_stamp DATE,
  time_stamp TIME,
  dw_last_update_date_time DATETIME NOT NULL
)
