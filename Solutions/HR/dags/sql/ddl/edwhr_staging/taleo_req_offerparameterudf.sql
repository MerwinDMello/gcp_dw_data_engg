CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_req_offerparameterudf
(
  file_date DATE,
  reqofferparameter_number INT64,
  udfdefinition_entity STRING,
  udfdefinition_id STRING,
  sequence INT64,
  udselement_number INT64,
  value STRING,
  dw_last_update_date_time DATETIME
);