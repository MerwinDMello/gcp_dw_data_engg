CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.recruitment_job_detail_wrk
(
  recruitment_job_sid INT64,
  element_detail_entity_text STRING,
  element_detail_type_text STRING,
  element_detail_seq_num INT64,
  valid_from_date DATETIME,
  valid_to_date DATETIME,
  element_detail_id INT64,
  element_detail_value_text STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);