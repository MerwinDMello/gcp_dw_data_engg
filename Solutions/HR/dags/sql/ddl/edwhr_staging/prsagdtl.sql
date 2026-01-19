create table if not exists `{{ params.param_hr_stage_dataset_name }}.prsagdtl`
(
  pay_step INT64 NOT NULL,
  pay_grade STRING NOT NULL,
  effect_date DATE NOT NULL,
  r_schedule STRING NOT NULL,
  company INT64 NOT NULL,
  base_pay_rate NUMERIC,
  grade_seq INT64,
  old_pay_rate NUMERIC,
  pay_rate NUMERIC,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
