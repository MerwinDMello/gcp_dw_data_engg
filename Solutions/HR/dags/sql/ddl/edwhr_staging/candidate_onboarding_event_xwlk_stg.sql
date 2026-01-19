CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_event_xwlk_stg
(
    requisition_num STRING,
    candidate_num STRING,
    event_type_id STRING,
    recruitment_requisition_num_text STRING,
    creation_date_time DATETIME,
    candidate_sid INT64,
    dw_last_update_date_time DATETIME
);