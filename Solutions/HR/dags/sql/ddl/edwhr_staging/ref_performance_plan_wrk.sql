CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.ref_performance_plan_wrk
(
  performance_plan_desc STRING NOT NULL,
  source_system_code STRING NOT NULL,
  dw_last_update_time DATETIME NOT NULL
);
