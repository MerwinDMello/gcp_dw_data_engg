CREATE TABLE IF NOT EXISTS `{{ params.param_hr_stage_dataset_name }}.time_entry_pay_code_detail_wrk`
(
  employee_num INT64 NOT NULL,
  kronos_num NUMERIC(29) NOT NULL,
  clock_library_code STRING NOT NULL,
  kronos_pay_code_seq_num INT64 NOT NULL,
  valid_from_date DATETIME NOT NULL,
  valid_to_date DATETIME,
  kronos_pay_code STRING,
  rounded_clocked_hour_num NUMERIC(32, 3),
  pay_summary_group_code STRING,
  lawson_company_num INT64 NOT NULL,
  process_level_code STRING NOT NULL,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
);