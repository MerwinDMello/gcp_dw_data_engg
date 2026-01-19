CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.employee_edu_hist_rpt_wrk
(
  employee_education_profile_sid NUMERIC,
  employee_num STRING,
  employee_sid INT64,
  employee_talent_profile_sid INT64,
  school_name STRING,
  school_type STRING,
  degree STRING,
  major STRING,
  education_start_date STRING,
  education_end_date STRING,
  year_graduated STRING,
  gpa STRING,
  education_comments STRING,
  detail_value_date DATE,
  lawson_company_num STRING,
  process_level_code STRING,
  source_system_key STRING,
  source_system_code STRING,
  dw_last_update_date_tim DATETIME
);
