create table if not exists `{{ params.param_hr_stage_dataset_name }}.ref_address_type_stg`
(
  addr_type_code STRING NOT NULL,
  addr_type_desc STRING NOT NULL,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
)
