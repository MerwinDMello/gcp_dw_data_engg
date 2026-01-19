CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.rect_job_param_detail_wrk
(
  file_date DATE,
  recruitment_job_parameter_sid INT64,
  element_detail_entity_text STRING,
  element_detail_type_text STRING,
  element_detail_seq INT64,
  job_parameter_num INT64,
  element_detail_id INT64,
  element_detail_value_text STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);