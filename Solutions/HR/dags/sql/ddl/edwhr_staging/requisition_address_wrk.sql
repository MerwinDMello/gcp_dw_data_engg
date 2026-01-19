create table if not exists `{{ params.param_hr_stage_dataset_name }}.requisition_address_wrk`
(
  requisition_sid INT64 NOT NULL,
  addr_sid INT64 NOT NULL,
  valid_from_date DATETIME NOT NULL,
  valid_to_date DATETIME NOT NULL,
  requisition_num INT64 NOT NULL,
  lawson_company_num INT64 NOT NULL,
  process_level_code STRING NOT NULL,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
