CREATE TABLE IF NOT EXISTS `{{ params.param_hr_stage_dataset_name }}.time_entry_pay_code_detail_stg`
(
  kronos_clock_library STRING NOT NULL,
  employee_num INT64 NOT NULL,
  kronos_time_id NUMERIC(29) NOT NULL,
  seq_kronos_pay_code_seq_num INT64 NOT NULL,
  kronos_pay_code STRING,
  pay_summary_group STRING,
  hours NUMERIC(32, 3),
  hr_company INT64,
  process_level STRING NOT NULL,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);