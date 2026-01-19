CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.junc_student_exam_wrk
(
  student_sid INT64 NOT NULL,
  exam_id INT64 NOT NULL,
  valid_from_date DATETIME NOT NULL,
  valid_to_date DATETIME,
  exam_date DATE,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
);