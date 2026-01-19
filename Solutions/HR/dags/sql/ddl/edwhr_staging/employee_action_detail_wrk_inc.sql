create table if not exists `{{ params.param_hr_stage_dataset_name }}.employee_action_detail_wrk_inc`
(
  employee_sid INT64 NOT NULL,
  hr_company_sid INT64 NOT NULL,
  company INT64 NOT NULL,
  action_code STRING NOT NULL,
  effect_date DATE NOT NULL,
  action_nbr INT64 NOT NULL,
  employee INT64 NOT NULL,
  ant_end_date DATE,
  reason_01 STRING,
  user_id STRING,
  date_stamp DATE,
  action_type STRING,
  description STRING,
  pos_level INT64,
  lawson_company_num INT64 NOT NULL,
  process_level_code STRING NOT NULL,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
)
