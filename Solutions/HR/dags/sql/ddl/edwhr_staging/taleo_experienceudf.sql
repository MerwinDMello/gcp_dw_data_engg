CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_experienceudf
(
  file_date DATE,
  experience_number NUMERIC,
  udfdefinition_entity STRING,
  udfdefinition_id STRING,
  sequence NUMERIC,
  udselement_number NUMERIC,
  value STRING,
  dw_last_update_date_time DATETIME
);