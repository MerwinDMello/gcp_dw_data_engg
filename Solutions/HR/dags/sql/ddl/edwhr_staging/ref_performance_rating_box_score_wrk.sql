CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.ref_performance_rating_box_score_wrk
(
  performance_rating_id INT64 NOT NULL,
  performance_potential_id INT64 NOT NULL,
  overall_performance_rating_desc STRING,
  performance_potential_desc STRING,
  box_score_num INT64,
  box_score_desc STRING,
  box_score_group_desc STRING,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
);