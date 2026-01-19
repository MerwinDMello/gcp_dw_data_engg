create table if not exists `{{ params.param_hr_stage_dataset_name }}.requisition_status_wrk`
(
  requisition_sid INT64 NOT NULL,
  status_sid INT64 NOT NULL,
  eff_from_date DATE NOT NULL,
  eff_to_date DATE NOT NULL,
  requisition_num INT64 NOT NULL,
  status_code STRING NOT NULL,
  lawson_company_num INT64 NOT NULL,
  process_level_code STRING NOT NULL,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
