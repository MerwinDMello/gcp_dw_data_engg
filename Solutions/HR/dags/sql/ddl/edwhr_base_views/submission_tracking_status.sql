create or replace view `{{ params.param_hr_base_views_dataset_name }}.submission_tracking_status`
AS SELECT
    submission_tracking_status.submission_tracking_sid,
    submission_tracking_status.valid_from_date,
    submission_tracking_status.valid_to_date,
    submission_tracking_status.submission_status_id,
    submission_tracking_status.source_system_code,
    submission_tracking_status.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.submission_tracking_status;