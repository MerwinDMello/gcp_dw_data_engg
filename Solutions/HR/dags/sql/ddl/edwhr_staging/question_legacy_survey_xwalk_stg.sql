CREATE  TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.question_legacy_survey_xwalk_stg
(
question_id STRING NOT NULL 
, measure_id_text STRING 
, legacy_question_id INT64 
, domain_id INT64
, domain_group_id INT64 
, source_system_code STRING 
, dw_last_update_date_time DATETIME 
)
CLUSTER BY question_id;