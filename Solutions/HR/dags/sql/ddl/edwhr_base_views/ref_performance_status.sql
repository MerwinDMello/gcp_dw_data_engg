CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_performance_status
AS SELECT
    ref_performance_status.performance_status_id,
    ref_performance_status.performance_status_desc,
    ref_performance_status.source_system_code,
    ref_performance_status.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_performance_status
	;