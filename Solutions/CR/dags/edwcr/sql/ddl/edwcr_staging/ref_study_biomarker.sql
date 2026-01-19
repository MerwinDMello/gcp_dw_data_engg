CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.ref_study_biomarker (
bio_marker_id INT64 NOT NULL
, source_bio_marker_code INT64 NOT NULL
, bio_marker_name STRING
, bio_marker_desc STRING
, is_diry_ind STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
