create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_submission_status`
AS SELECT
    ref_submission_status.submission_status_id,
    ref_submission_status.active_sw,
    ref_submission_status.submission_state_id,
    ref_submission_status.submission_status_code,
    ref_submission_status.submission_status_name,
    ref_submission_status.submission_status_desc,
    ref_submission_status.source_system_code,
    ref_submission_status.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.ref_submission_status;