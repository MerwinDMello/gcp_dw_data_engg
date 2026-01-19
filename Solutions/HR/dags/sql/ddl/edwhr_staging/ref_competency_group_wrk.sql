CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.ref_competency_group_wrk
(
  competency_group_id INT64 NOT NULL,
  competency_group_desc STRING NOT NULL,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
);
