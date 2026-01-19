CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_requisitionhistory
(
  file_date DATE,
  requisition_number INT64,
  creationdate DATETIME,
  requisitionstate_number INT64,
  enddate DATETIME,
  creator_userno INT64,
  recruiterowner_userno INT64,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);