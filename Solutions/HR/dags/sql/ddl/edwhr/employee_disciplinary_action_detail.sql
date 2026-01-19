create table if not exists `{{ params.param_hr_core_dataset_name }}.employee_disciplinary_action_detail`
(
  employee_sid INT64 NOT NULL OPTIONS(description="unique numeric number generated for unique combination of employee identifier and hr company identifier"),
  disciplinary_ind STRING NOT NULL OPTIONS(description="indicates whether the action is a grievance (1) or disciplinary (2) issue."),
  disciplinary_action_num INT64 NOT NULL OPTIONS(description="contains the grievance or disciplinary action number."),
  disciplinary_seq_num INT64 NOT NULL OPTIONS(description="automatically assigned sequence number from lawson for multiple records for the same grievance or disciplinary action for an employee."),
  valid_from_date DATETIME NOT NULL OPTIONS(description="date on which the record became valid. load date typically."),
  valid_to_date DATETIME OPTIONS(description="date on which the record was invalidated."),
  eff_from_date DATE OPTIONS(description="contains the date for the grievance or disciplinary action follow up step."),
  status_flag STRING NOT NULL OPTIONS(description="contains the grievance or disciplinary action follow up step of either 1 (open), 2 (in process), or 3 (completed)."),
  follow_up_category_name STRING OPTIONS(description="contains the user-defined grievance or disciplinary action step category."),
  follow_up_type_name STRING OPTIONS(description="contains the user-defined grievance or disciplinary action step type."),
  follow_up_outcome_desc STRING OPTIONS(description="contains the user-defined grievance or  disciplinary action step outcome."),
  follow_up_performed_by_employee_sid INT64 OPTIONS(description="employee sid for the employee who performed the action follow up step."),
  follow_up_comment_desc STRING OPTIONS(description="comments entered by the user."),
  last_update_date DATE NOT NULL OPTIONS(description="last date a record was updated in lawson."),
  last_update_user_34_login_code STRING OPTIONS(description="user that made last update to record."),
  employee_num INT64 NOT NULL OPTIONS(description="employee number from lawson unique to each employee."),
  lawson_company_num INT64 NOT NULL OPTIONS(description="unique four digit numeric value of lawson generated hr company maintained in this field"),
  process_level_code STRING NOT NULL OPTIONS(description="unique process level code of an hr company value mainatined in this field."),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
CLUSTER BY employee_sid
OPTIONS(
  description="this table contains the disciplinary actions of employees."
);