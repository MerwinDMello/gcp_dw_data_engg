CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.additional_education_history
(
  employee_id STRING,
  spoken_languages STRING,
  written_languages STRING,
  prof_org_free_text STRING,
  licenses_certifications STRING,
  skills_experience STRING,
  special_training STRING,
  passionate_job_functions STRING,
  successful_job_functions STRING,
  edu_hist_record_id STRING,
  dw_last_update_date_time DATETIME NOT NULL
);

