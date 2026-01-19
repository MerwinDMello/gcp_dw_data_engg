CREATE TABLE IF NOT EXISTS  {{ params.param_hr_stage_dataset_name }}.student_program_graduation_wrk
(
  student_sid INT64 NOT NULL,
  nursing_program_id INT64 NOT NULL,
  valid_from_date DATETIME NOT NULL,
  graduation_date DATE NOT NULL,
  cumulative_gpa NUMERIC(31, 2),
  nursing_school_id INT64,
  campus_id INT64,
  valid_to_date DATETIME,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);