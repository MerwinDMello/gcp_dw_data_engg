create table if not exists `{{ params.param_hr_core_dataset_name }}.phone_country`
(
  country_code STRING NOT NULL,
  country_name STRING NOT NULL,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
CLUSTER BY country_code;