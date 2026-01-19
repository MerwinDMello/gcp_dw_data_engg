create table if not exists `{{ params.param_hr_core_dataset_name }}.junc_employee_status`
(
  employee_sid INT64 NOT NULL OPTIONS(description="unique numeric number generated for unique combination of employee identifier and hr company identifier"),
  status_sid INT64 NOT NULL OPTIONS(description="unique identifier generated for values of an status code"),
  valid_from_date DATETIME NOT NULL OPTIONS(description="date on which the record became valid. load date typically."),
  status_type_code STRING OPTIONS(description="it captures employee status type codes.aux : auxiliary status typeemp: employee status type"),
  valid_to_date DATETIME OPTIONS(description="date on which the record was invalidated."),
  employee_num INT64 NOT NULL OPTIONS(description="employee number of an employee associated with hr lawson company"),
  status_code STRING NOT NULL OPTIONS(description="this represents the available code values for the status of the employee or applicant. "),
  lawson_company_num INT64 NOT NULL OPTIONS(description="the number that identifies a company.a company represents a business or legal entity of an organization"),
  process_level_code STRING NOT NULL OPTIONS(description="unique process level code of an hr company value mainatined in this field."),
  delete_ind STRING OPTIONS(description="the indicator is usually a y/ n (yes/no) designation of a record but can sometimes be other values.  this should not be 1/0 designation."),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
PARTITION BY DATE(valid_from_date)
CLUSTER BY employee_sid, status_sid
OPTIONS(
  description="current and previous status of an employee while associated with an hr company maintained"
);