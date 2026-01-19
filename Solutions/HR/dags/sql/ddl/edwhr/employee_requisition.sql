create table if not exists `{{ params.param_hr_core_dataset_name }}.employee_requisition`
(
  employee_sid INT64 NOT NULL OPTIONS(description="system generated unique value for each combination of employee number and lawson company number."),
  requisition_sid INT64 NOT NULL OPTIONS(description="unique id of an lawson generated requisition."),
  action_type_code STRING NOT NULL OPTIONS(description="it represents the various action type code values availabe for personnel action. valid values are as follow  a = hire an applicant,e = individual action,m = mass change,p = mass pay change,l = multiple positions"),
  eff_from_date DATE NOT NULL OPTIONS(description="represents the date of the employee requisition action"),
  valid_from_date DATETIME NOT NULL OPTIONS(description="date on which the record became valid. load date typically."),
  valid_to_date DATETIME OPTIONS(description="date on which the record was invalidated."),
  action_code STRING OPTIONS(description="contains the action type associated with the personnel action."),
  user_id_text STRING OPTIONS(description="identifying information about the user"),
  work_unit_num NUMERIC OPTIONS(description="the work unit number associated with that action."),
  lawson_company_num INT64 NOT NULL OPTIONS(description="the number that identifies a company. a company represents a business or legal entity of an organization"),
  process_level_code STRING NOT NULL OPTIONS(description="unique process level code of an hr company value mainatined in this field."),
  requisition_num INT64 OPTIONS(description="lawson generated requisition number"),
  employee_num INT64 OPTIONS(description="lawson generated employee number"),
  delete_ind STRING OPTIONS(description="the indicator is usually a y/ n (yes/no) designation of a record but can sometimes be other values.  this should not be 1/0 designation."),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
CLUSTER BY employee_sid, requisition_sid, action_type_code, valid_from_date
OPTIONS(
  description="this tables contains information about the employee and their requisitions."
);