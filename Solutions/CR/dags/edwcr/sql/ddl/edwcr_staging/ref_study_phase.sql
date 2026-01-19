CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.ref_study_phase (
phase_id INT64 NOT NULL
, phase_source_code INT64 NOT NULL
, phase_name STRING NOT NULL
, phase_desc STRING
, is_dirty_ind STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
