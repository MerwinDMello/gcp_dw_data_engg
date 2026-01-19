CREATE TABLE IF NOT EXISTS `{{ params.param_hr_stage_dataset_name }}.pay_code_pay_summary_group_crosswalk_stg`
(
  kronos_clock_library STRING NOT NULL,
  pay_code STRING NOT NULL,
  pay_code_desc STRING,
  kronos_payroll_interface_code STRING,
  pay_summary_group STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);