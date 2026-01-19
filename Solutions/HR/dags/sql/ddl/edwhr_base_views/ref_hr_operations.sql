create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_hr_operations`
AS SELECT
  ref_hr_operations.process_level_code,
  ref_hr_operations.business_unit_name,
  ref_hr_operations.business_unit_segment_name,
  ref_hr_operations.source_system_code,
  ref_hr_operations.dw_last_update_date_time
FROM
 {{ params.param_hr_core_dataset_name }}.ref_hr_operations;