CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.job_template
(
  job_template_sid INT64 NOT NULL,
  valid_from_date DATETIME NOT NULL,
  job_template_num INT64,
  base_job_template_num INT64,
  recruitment_job_sid INT64,
  job_template_status_id INT64,
  valid_to_date DATETIME,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
CLUSTER BY job_template_sid;