CREATE  OR REPLACE TABLE {{ params.param_cr_audit_dataset_name }}.data_validation_result
(
  source_project STRING,
  source_dataset STRING,
  target_project STRING,
  target_dataset STRING,
  table STRING,
  comparison STRING,
  metric STRING,
  data NUMERIC,
  remarks STRING,
  validation_time DATETIME
);