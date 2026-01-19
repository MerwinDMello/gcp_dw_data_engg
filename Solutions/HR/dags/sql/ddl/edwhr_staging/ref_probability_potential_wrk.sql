CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.ref_probability_potential_wrk
(
  probability_potential_id INT64 NOT NULL,
  probability_potential_desc STRING NOT NULL,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
);
