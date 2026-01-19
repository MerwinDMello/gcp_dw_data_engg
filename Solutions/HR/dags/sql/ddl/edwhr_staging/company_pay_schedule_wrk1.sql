create table if not exists `{{ params.param_hr_stage_dataset_name }}.company_pay_schedule_wrk1`
(
  sk NUMERIC,
  hr_company_sid INT64,
  company INT64 NOT NULL,
  pay_schedule_code STRING NOT NULL,
  pay_schedule_flag STRING NOT NULL,
  pay_schedule_eff_date DATE NOT NULL,
  pay_schedule_desc STRING,
  salary_class_flag STRING,
  last_grade_sequence_num INT64,
  pay_schedule_status_ind INT64,
  currency_code STRING,
  currency_nd INT64,
  security_key_text STRING,
  eff_from_date DATE,
  valid_from_date DATETIME,
  eff_to_date DATE,
  active_dw_ind STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
)
