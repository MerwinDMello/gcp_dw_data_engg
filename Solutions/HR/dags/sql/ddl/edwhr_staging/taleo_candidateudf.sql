CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_candidateudf
(
  file_date DATE,
  candidate_number INT64 NOT NULL,
  udfdefinition_entity STRING,
  udfdefinition_id STRING,
  sequence INT64 NOT NULL,
  udselement_number INT64,
  value STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
)
;