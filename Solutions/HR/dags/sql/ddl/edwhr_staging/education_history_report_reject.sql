CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.education_history_report_reject
(
  employee_id STRING,
  school_name STRING,
  school_type STRING,
  degree STRING,
  major STRING,
  education_start_date STRING,
  education_end_date STRING,
  year_graduated STRING,
  gpa STRING,
  education_comments STRING,
  edu_hist_record_id STRING,
  dw_last_update_date_time DATETIME,
  reject_reason STRING,
  reject_stg_tbl_nm STRING
);
