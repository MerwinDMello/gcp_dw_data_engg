CREATE TABLE IF NOT EXISTS {{params.param_hr_core_dataset_name}}.hr_secref_process_level
(
  company_code STRING,
  user_id STRING NOT NULL,
  lawson_company_num INT64 NOT NULL,
  process_level_code STRING NOT NULL
)
CLUSTER BY user_id, lawson_company_num, process_level_code;