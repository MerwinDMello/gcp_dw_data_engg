create table if not exists `{{ params.param_hr_stage_dataset_name }}.prsaghead`
(
  company INT64 NOT NULL,
  effect_date DATE NOT NULL,
  r_indicator STRING NOT NULL,
  r_schedule STRING NOT NULL,
  base_currency STRING,
  base_nd INT64,
  currency_code STRING,
  curr_nd INT64,
  description STRING,
  lst_grade_seq INT64,
  salary_class STRING,
  sghset3_ss_sw STRING,
  status INT64,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
