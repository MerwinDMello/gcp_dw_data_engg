CREATE OR REPLACE VIEW {{params.param_hr_base_views_dataset_name}}.ref_submission_event
AS SELECT
    ref_submission_event.submission_event_id,
    ref_submission_event.submission_event_category_id,
    ref_submission_event.submission_event_code,
    ref_submission_event.submission_event_desc,
    ref_submission_event.submission_event_detail_desc,
    ref_submission_event.source_system_code,
    ref_submission_event.dw_last_update_date_time
  FROM
    {{params.param_hr_core_dataset_name}}.ref_submission_event;