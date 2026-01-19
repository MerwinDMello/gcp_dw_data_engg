CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_performance_category
AS SELECT
    ref_performance_category.performance_category_id,
    ref_performance_category.performance_category_desc,
    ref_performance_category.source_system_code,
    ref_performance_category.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_performance_category;
