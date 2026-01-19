create table if not exists {{ params.param_hr_stage_dataset_name }}.emp_disciplinary_action_wrk (
employee_sid int64 not null
, disciplinary_ind string not null
, disciplinary_action_num int64 not null
, valid_from_date datetime not null
, valid_to_date datetime
, disciplinary_desc string
, creation_date date
, action_category_code string
, report_date date
, reported_by_employee_num int64
, reported_by_name string
, action_status_code string
, action_outcome_desc string
, action_outcome_date date
, days_out_cnt int64
, department_sid int64
, location_code string
, job_code_sid int64
, comment_desc string
, supervisor_employee_num int64
, last_update_date date
, last_update_user_34_login_code string
, employee_num int64 not null
, lawson_company_num int64 not null
, process_level_code string not null
, source_system_code string not null
, dw_last_update_date_time datetime not null
)
  ;
