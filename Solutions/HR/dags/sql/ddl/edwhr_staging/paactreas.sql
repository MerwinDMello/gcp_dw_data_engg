create table if not exists `{{ params.param_hr_stage_dataset_name }}.paactreas`
(
  action_code STRING,
  active_flag STRING,
  act_reason_cd STRING,
  company INT64,
  creset3_ss_sw STRING,
  creset4_ss_sw STRING,
  creset5_ss_sw STRING,
  date_stamp DATE,
  description STRING,
  time_stamp STRING,
  user_id STRING,
  dw_last_update_date_time DATETIME
)
