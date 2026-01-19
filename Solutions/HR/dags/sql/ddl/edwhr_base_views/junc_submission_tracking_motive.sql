create or replace view `{{ params.param_hr_base_views_dataset_name }}.junc_submission_tracking_motive`
AS SELECT
    junc_submission_tracking_motive.submission_tracking_sid,
    junc_submission_tracking_motive.tracking_motive_id,
    junc_submission_tracking_motive.valid_from_date,
    junc_submission_tracking_motive.valid_to_date,
    junc_submission_tracking_motive.source_system_code,
    junc_submission_tracking_motive.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.junc_submission_tracking_motive;