create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_submission_state`
AS SELECT
    ref_submission_state.submission_state_id,
    ref_submission_state.submission_state_desc,
    ref_submission_state.source_system_code,
    ref_submission_state.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_submission_state;