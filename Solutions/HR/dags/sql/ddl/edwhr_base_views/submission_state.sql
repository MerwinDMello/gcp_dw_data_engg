create or replace view `{{ params.param_hr_base_views_dataset_name }}.submission_state`
AS SELECT
    submission_state.submission_sid,
    submission_state.valid_from_date,
    submission_state.submission_state_id,
    submission_state.valid_to_date,
    submission_state.source_system_code,
    submission_state.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.submission_state;