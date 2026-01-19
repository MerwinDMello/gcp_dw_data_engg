CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.fact_employee_availability_wrk
(
  employee_talent_profile_sid INT64 NOT NULL,
  employee_sid INT64,
  employee_num INT64,
  valid_from_date DATETIME NOT NULL,
  valid_to_date DATETIME,
  jobs_pooled_for_cnt INT64,
  employee_talent_pool_cnt INT64,
  employee_successor_cnt INT64,
  employee_ready_now_cnt INT64,
  employee_ready_18_24_month_cnt INT64,
  employee_ready_12_18_month_cnt INT64,
  employee_ready_6_11_month_cnt INT64,
  employee_other_readiness_cnt INT64,
  employee_readiness_unknown_cnt INT64,
  empl_slated_for_position_cnt INT64,
  emp_talent_pooled_for_pos_cnt INT64,
  lawson_company_num INT64,
  process_level_code STRING,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
);
