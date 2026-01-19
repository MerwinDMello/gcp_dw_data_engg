CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.secref_facility
(
  company_code STRING,
  user_id STRING NOT NULL,
  co_id STRING NOT NULL
)
CLUSTER BY company_code, user_id, co_id;
