CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_matched_requisition
(
  app_number INT64,
  requisition_number INT64,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);