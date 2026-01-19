create table if not exists `{{ params.param_hr_stage_dataset_name }}.employee_requisition_wrk`
(
  employee_sid INT64,
  requisition_sid INT64,
  action_type_code STRING,
  eff_from_date DATE,
  valid_from_date DATETIME,
  valid_to_date DATETIME,
  action_code STRING,
  user_id_text STRING,
  work_unit_num NUMERIC,
  delete_ind STRING,
  lawson_company_num INT64,
  process_level_code STRING,
  requisition_num INT64,
  employee_num INT64,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
)
