create table if not exists `{{ params.param_hr_stage_dataset_name }}.hrsuper`
(
  code STRING NOT NULL,
  company INT64 NOT NULL,
  active_flag STRING,
  description STRING,
  effect_date DATE,
  employee INT64,
  hsuset4_ss_sw STRING,
  obj_id NUMERIC,
  super_rpts_to STRING,
  user1 STRING,
  user2 STRING,
  user3 STRING,
  user4 STRING,
  user5 STRING,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
