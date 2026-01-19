create table if not exists `{{ params.param_hr_stage_dataset_name }}.ref_disciplinary_indicator_wrk`
(
  disciplinary_ind STRING NOT NULL,
  disciplinary_desc STRING,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
