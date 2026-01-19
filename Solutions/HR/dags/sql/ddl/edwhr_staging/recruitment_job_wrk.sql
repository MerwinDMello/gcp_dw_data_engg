CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.recruitment_job_wrk
(
  file_date DATE,
  recruitment_job_sid INT64,
  recruitment_job_num INT64,
  job_title_name STRING,
  job_grade_code STRING,
  job_schedule_id INT64,
  overtime_status_id INT64,
  primary_facility_location_num NUMERIC,
  recruiter_user_sid INT64,
  recruitment_job_parameter_sid INT64,
  recruitment_position_sid INT64,
  fte_pct NUMERIC,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);