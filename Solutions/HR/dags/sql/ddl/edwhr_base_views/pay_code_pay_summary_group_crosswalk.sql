CREATE OR REPLACE VIEW `{{ params.param_hr_base_views_dataset_name }}.pay_code_pay_summary_group_crosswalk`
AS SELECT
  pay_code_pay_summary_group_crosswalk.clock_library_code,
  pay_code_pay_summary_group_crosswalk.kronos_pay_code,
  pay_code_pay_summary_group_crosswalk.kronos_pay_code_desc,
  pay_code_pay_summary_group_crosswalk.lawson_pay_summary_group_code,
  pay_code_pay_summary_group_crosswalk.lawson_pay_code,
  pay_code_pay_summary_group_crosswalk.source_system_code,
  pay_code_pay_summary_group_crosswalk.dw_last_update_date_time
FROM
  {{ params.param_hr_core_dataset_name }}.pay_code_pay_summary_group_crosswalk;