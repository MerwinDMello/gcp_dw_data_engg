create table if not exists `{{ params.param_hr_core_dataset_name }}.ref_account_unit`
(
  account_unit_code STRING NOT NULL,
  account_unit_desc STRING,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
CLUSTER BY account_unit_code;