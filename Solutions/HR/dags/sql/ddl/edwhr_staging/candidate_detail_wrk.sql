CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.candidate_detail_wrk
(
  file_date DATE,
  candidate_sid INT64 NOT NULL,
  element_detail_entity_text STRING NOT NULL,
  element_detail_type_text STRING NOT NULL,
  element_detail_seq INT64 NOT NULL,
  valid_from_date DATETIME NOT NULL,
  valid_to_date DATETIME,
  element_detail_id NUMERIC(29),
  element_detail_value_text STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
)
;