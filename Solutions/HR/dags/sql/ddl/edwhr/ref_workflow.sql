CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_workflow
(
  workflow_id INT64 NOT NULL,
  active_sw INT64,
  workflow_code STRING,
  workflow_name STRING,
  workflow_desc STRING,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
CLUSTER BY workflow_id;
