CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_performance_period
AS SELECT
    ref_performance_period.review_period_id,
    ref_performance_period.review_period_desc,
    ref_performance_period.source_system_code,
    ref_performance_period.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_performance_period;