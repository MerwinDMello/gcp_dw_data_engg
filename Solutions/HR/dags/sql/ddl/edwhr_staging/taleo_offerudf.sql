CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_offerudf
(
  file_date DATE,
  offer_number STRING,
  udfdefinition_entity STRING,
  udfdefinition_id STRING,
  sequence STRING,
  udselement_number STRING,
  value STRING
)
;
