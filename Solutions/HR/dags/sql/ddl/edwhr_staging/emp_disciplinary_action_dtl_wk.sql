create table if not exists `{{ params.param_hr_stage_dataset_name }}.emp_disciplinary_action_dtl_wk`
(
  employee_sid INT64 NOT NULL,
  disciplinary_ind STRING NOT NULL,
  disciplinary_action_num INT64 NOT NULL,
  disciplinary_seq_num INT64 NOT NULL,
  valid_from_date DATETIME NOT NULL,
  valid_to_date DATETIME,
  eff_from_date DATE,
  status_flag STRING NOT NULL,
  follow_up_category_name STRING,
  follow_up_type_name STRING,
  follow_up_outcome_desc STRING,
  follow_up_performed_by_employee_sid INT64,
  follow_up_comment_desc STRING,
  last_update_date DATE NOT NULL,
  last_update_user_34_login_code STRING,
  employee_num INT64 NOT NULL,
  lawson_company_num INT64 NOT NULL,
  process_level_code STRING NOT NULL,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
