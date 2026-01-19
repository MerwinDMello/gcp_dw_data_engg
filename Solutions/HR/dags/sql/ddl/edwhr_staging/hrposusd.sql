create table if not exists `{{ params.param_hr_stage_dataset_name }}.hrposusd`
(
  company INT64 NOT NULL,
  hrpousd_type STRING NOT NULL,
  obj_id NUMERIC NOT NULL,
  field_key STRING NOT NULL,
  effect_date DATE,
  end_date DATE,
  a_field STRING NOT NULL,
  n_field NUMERIC NOT NULL,
  d_field DATE,
  date_stamp DATE,
  time_stamp TIME NOT NULL,
  user_id STRING NOT NULL,
  currency_code STRING NOT NULL,
  curr_nd INT64 NOT NULL,
  base_currency STRING NOT NULL,
  base_nd INT64 NOT NULL,
  base_amount NUMERIC NOT NULL,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
)
