create table if not exists `{{ params.param_hr_stage_dataset_name }}.deptcode`
(
  department STRING NOT NULL,
  process_level STRING NOT NULL,
  company INT64 NOT NULL,
  active_flag STRING,
  date_stamp DATE,
  dep_account INT64,
  dep_acct_unit STRING,
  dep_dist_co INT64,
  dep_sub_acct INT64,
  name STRING,
  sec_location STRING,
  sec_lvl INT64,
  segment_flag STRING,
  time_stamp TIME,
  user_id STRING,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
