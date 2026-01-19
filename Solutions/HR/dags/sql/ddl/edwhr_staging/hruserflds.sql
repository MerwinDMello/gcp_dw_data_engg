create table if not exists `{{ params.param_hr_stage_dataset_name }}.hruserflds`
(
  field_key STRING NOT NULL,
  active_flag STRING,
  beg_date DATE,
  beg_number NUMERIC,
  currency_flag STRING,
  end_date DATE,
  end_number NUMERIC,
  field_name STRING,
  field_type STRING,
  hruset3_ss_sw STRING,
  hruset4_ss_sw STRING,
  log_flag STRING,
  pers_action STRING,
  req_flag STRING,
  req_value STRING,
  r_indicator STRING,
  sec_level INT64,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
