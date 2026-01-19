create table if not exists `{{ params.param_hr_stage_dataset_name }}.hrposflds`
(
  hrposflds_type STRING NOT NULL,
  field_key STRING NOT NULL,
  field_name STRING NOT NULL,
  field_type STRING NOT NULL,
  beg_number NUMERIC NOT NULL,
  end_number NUMERIC NOT NULL,
  beg_date DATE,
  end_date DATE,
  req_flag STRING NOT NULL,
  req_value STRING NOT NULL,
  log_flag STRING NOT NULL,
  date_stamp DATE,
  time_stamp TIME,
  user_id STRING NOT NULL,
  currency_flag STRING NOT NULL,
  active_flag STRING NOT NULL,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
)
