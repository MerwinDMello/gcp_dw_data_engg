CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_recruitment_source_stg
(
  file_date DATE,
  recruitment_source_number STRING,
  description STRING,
  networklocation_number STRING,
  objectorigin_number STRING,
  parentsource_number STRING,
  state_number STRING,
  type_number STRING,
  visibility_number STRING,
  partnerid STRING,
  dw_last_update_date_time DATETIME
)
;