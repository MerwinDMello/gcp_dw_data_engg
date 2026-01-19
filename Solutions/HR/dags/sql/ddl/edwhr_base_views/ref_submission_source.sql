CREATE OR REPLACE VIEW {{params.param_hr_base_views_dataset_name}}.ref_submission_source
AS SELECT
    ref_submission_source.submission_source_code,
    ref_submission_source.submission_source_desc,
    ref_submission_source.source_system_code,
    ref_submission_source.dw_last_update_date_time
  FROM
    {{params.param_hr_core_dataset_name}}.ref_submission_source;