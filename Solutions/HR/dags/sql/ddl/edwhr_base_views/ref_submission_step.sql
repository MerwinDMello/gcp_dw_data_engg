create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_submission_step`
AS SELECT
    ref_submission_step.step_id,
    ref_submission_step.submission_state_id,
    ref_submission_step.active_sw,
    ref_submission_step.step_code,
    ref_submission_step.step_name,
    ref_submission_step.step_short_name,
    ref_submission_step.step_desc,
    ref_submission_step.source_system_code,
    ref_submission_step.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_submission_step;