create table if not exists `{{ params.param_hr_stage_dataset_name }}.hrctrycode`
(
  hrctry_code STRING NOT NULL,
  country_code STRING NOT NULL,
  type STRING NOT NULL,
  active_flag STRING,
  date_stamp DATE,
  description STRING,
  recruit_avail STRING,
  time_stamp TIME,
  us_et_behavior STRING,
  user_id STRING,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
