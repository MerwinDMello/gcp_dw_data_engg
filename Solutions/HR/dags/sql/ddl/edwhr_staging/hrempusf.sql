create table if not exists `{{ params.param_hr_stage_dataset_name }}.hrempusf`
(
  field_key STRING NOT NULL,
  employee INT64 NOT NULL,
  emp_app INT64 NOT NULL,
  company INT64,
  a_field STRING,
  base_amount NUMERIC,
  base_currency STRING,
  base_nd INT64,
  currency_code STRING,
  curr_nd INT64,
  d_field DATE,
  heuset3_ss_sw STRING,
  n_field NUMERIC,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
