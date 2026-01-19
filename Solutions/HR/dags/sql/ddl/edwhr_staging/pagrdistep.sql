create table if not exists `{{ params.param_hr_stage_dataset_name }}.pagrdistep`
(
  company INT64,
  employee INT64,
  pagrdistep_type STRING,
  griev_dis_nbr INT64,
  seq_nbr INT64,
  effect_date DATE,
  status INT64,
  follow_cat STRING,
  follow_type STRING,
  outcome STRING,
  performed_by INT64,
  r_comment STRING,
  date_stamp DATE,
  time_stamp TIME,
  user_id STRING,
  l_index STRING,
  l_atpgi_ss_sw STRING,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
