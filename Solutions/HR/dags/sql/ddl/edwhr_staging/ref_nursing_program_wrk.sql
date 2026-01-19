CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.ref_nursing_program_wrk
(
  nursing_program_id INT64 NOT NULL,
  program_name STRING NOT NULL,
  program_type_code STRING,
  program_degree_text STRING,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME
);