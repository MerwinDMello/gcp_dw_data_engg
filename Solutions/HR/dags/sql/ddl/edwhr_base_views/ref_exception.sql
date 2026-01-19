CREATE OR REPLACE VIEW `{{ params.param_hr_base_views_dataset_name }}.ref_exception`
AS SELECT
  ref_exception.exception_code,
  ref_exception.exception_desc,
  ref_exception.source_system_code,
  ref_exception.dw_last_update_date_time
FROM
  {{ params.param_hr_core_dataset_name }}.ref_exception;