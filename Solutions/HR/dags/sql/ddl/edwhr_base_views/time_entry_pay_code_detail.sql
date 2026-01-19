CREATE OR REPLACE VIEW `{{ params.param_hr_base_views_dataset_name }}.time_entry_pay_code_detail`
AS SELECT
  time_entry_pay_code_detail.employee_num,
  time_entry_pay_code_detail.kronos_num,
  time_entry_pay_code_detail.clock_library_code,
  time_entry_pay_code_detail.kronos_pay_code_seq_num,
  time_entry_pay_code_detail.valid_from_date,
  time_entry_pay_code_detail.valid_to_date,
  time_entry_pay_code_detail.kronos_pay_code,
  time_entry_pay_code_detail.rounded_clocked_hour_num,
  time_entry_pay_code_detail.pay_summary_group_code,
  time_entry_pay_code_detail.lawson_company_num,
  time_entry_pay_code_detail.process_level_code,
  time_entry_pay_code_detail.source_system_code,
  time_entry_pay_code_detail.dw_last_update_date_time
FROM
  {{ params.param_hr_core_dataset_name }}.time_entry_pay_code_detail;