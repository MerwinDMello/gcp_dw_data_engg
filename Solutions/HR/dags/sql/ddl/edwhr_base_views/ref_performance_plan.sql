CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_performance_plan
AS SELECT
    ref_performance_plan.performance_plan_id,
    ref_performance_plan.performance_plan_desc,
    ref_performance_plan.source_system_code,
    ref_performance_plan.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_performance_plan;