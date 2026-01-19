CREATE OR REPLACE VIEW {{params.param_hr_base_views_dataset_name}}.ref_submission_event_category
AS SELECT
    ref_submission_event_category.submission_event_category_id,
    ref_submission_event_category.submission_event_category_code,
    ref_submission_event_category.submission_event_category_desc,
    ref_submission_event_category.source_system_code,
    ref_submission_event_category.dw_last_update_date_time
  FROM
    {{params.param_hr_core_dataset_name}}.ref_submission_event_category;