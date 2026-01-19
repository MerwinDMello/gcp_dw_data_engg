create table if not exists `{{ params.param_hr_stage_dataset_name }}.requisition_position_wrk`
(
  position_sid INT64,
  requisition_sid INT64,
  valid_from_date DATETIME NOT NULL,
  valid_to_date DATETIME,
  requisition_num INT64 NOT NULL,
  position_code STRING NOT NULL,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL,
  lawson_company_num INT64,
  process_level_code STRING
)
