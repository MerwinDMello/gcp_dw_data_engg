create table if not exists `{{ params.param_hr_stage_dataset_name }}.employee_requisition_comment_wrk`
(
  employee_sid INT64,
  requisition_sid INT64,
  applicant_type_id INT64,
  comment_type_code STRING,
  action_code STRING,
  comment_line_num INT64,
  sequence_num INT64,
  hr_company_sid INT64,
  valid_from_date DATETIME,
  lawson_company_num INT64,
  valid_to_date DATETIME,
  comment_text STRING,
  comment_date DATE,
  print_code STRING,
  process_level_code STRING,
  requisition_num INT64,
  employee_num INT64,
  delete_ind STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
)
