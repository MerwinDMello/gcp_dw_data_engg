create table if not exists `{{ params.param_hr_core_dataset_name }}.employee_detail`
(
  employee_detail_code STRING NOT NULL OPTIONS(description=" unique codes of user field applicable for an employee of an hr company"),
  employee_sid INT64 NOT NULL OPTIONS(description=" unique numeric number generated for unique combination of employee identifier and hr company identifier"),
  applicant_type_id INT64 NOT NULL OPTIONS(description=" indicates if the individual is an employee or an applicant.lawson system maintains 0 and 1 for employee and applicant respectively and same values are maintained in this field."),
  valid_from_date DATETIME NOT NULL OPTIONS(description=" date on which the record became valid. load date typically."),
  valid_to_date DATETIME OPTIONS(description=" date on which the record was invalidated."),
  detail_value_alphanumeric_text STRING OPTIONS(description=" alphanumeric value of an employee user field"),
  detail_value_num INT64 OPTIONS(description=" contains employee values for numeric user fields"),
  detail_value_date DATE OPTIONS(description=" employees date value of an user field"),
  employee_num INT64 OPTIONS(description=" this is an lawson employee number"),
  lawson_company_num INT64 NOT NULL OPTIONS(description=" the number that identifies a company.a company represents a business or legal entity of an organization"),
  process_level_code STRING NOT NULL OPTIONS(description=" unique process level code of an hr company value mainatined in this field."),
  delete_ind STRING OPTIONS(description=" the indicator is usually a y/ n (yes/no) designation of a record but can sometimes be other values.  this should not be 1/0 designation."),
  source_system_code STRING NOT NULL OPTIONS(description=" a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description=" datetime of update or load of this record to the enterprise data warehouse.")
)
PARTITION BY DATE(valid_from_date)
CLUSTER BY employee_detail_code, employee_sid, applicant_type_id
OPTIONS(
  description=" this table maintains employees various details like ssn, previous job experience etc are capture before or after hired "
);