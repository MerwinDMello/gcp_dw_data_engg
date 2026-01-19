create table if not exists `{{ params.param_hr_core_dataset_name }}.employee_supervisor`
(
  employee_sid INT64 NOT NULL OPTIONS(description="unique numeric number generated for unique combination of employee identifier and hr company identifier"),
  valid_from_date DATETIME NOT NULL OPTIONS(description="date on which the record became valid. load date typically."),
  supervisor_sid INT64 OPTIONS(description="etl generated unique numeric number assigned to each supervisor code"),
  valid_to_date DATETIME OPTIONS(description="date on which the record was invalidated."),
  employee_num INT64 NOT NULL OPTIONS(description="employee number of an employee associated with hr lawson company"),
  supervisor_code STRING NOT NULL OPTIONS(description="unique value of supervisor code"),
  lawson_company_num INT64 NOT NULL OPTIONS(description="the number that identifies a company.a company represents a business or legal entity of an organization"),
  process_level_code STRING NOT NULL OPTIONS(description="unique process level code of an hr company value mainatined in this field."),
  delete_ind STRING OPTIONS(description="the indicator is usually a y/ n (yes/no) designation of a record but can sometimes be other values.  this should not be 1/0 designation."),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
PARTITION BY DATE(valid_from_date)
CLUSTER BY employee_sid
OPTIONS(
  description="employee of an hr company who is assigned to the supervisor position"
);