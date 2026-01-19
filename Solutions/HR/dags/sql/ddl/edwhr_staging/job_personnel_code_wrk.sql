create table if not exists `{{ params.param_hr_stage_dataset_name }}.job_personnel_code_wrk`
(
  job_code_sid INT64,
  position_sid INT64,
  personnel_type_code STRING,
  personnel_code STRING,
  hr_company_sid INT64 NOT NULL,
  valid_from_date DATETIME,
  valid_to_date DATETIME,
  required_flag_ind STRING,
  personnel_code_time_pct NUMERIC,
  proficiency_level_desc STRING,
  weight_amt NUMERIC,
  subject_code STRING,
  job_code STRING,
  position_code STRING,
  lawson_company_num INT64,
  process_level_code STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
)
