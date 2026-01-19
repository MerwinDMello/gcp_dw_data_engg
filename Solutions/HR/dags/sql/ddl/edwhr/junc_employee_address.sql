create table if not exists `{{ params.param_hr_core_dataset_name }}.junc_employee_address`
(
  employee_sid INT64 NOT NULL OPTIONS(description=" unique numeric number generated for unique combination of employee identifier and hr company identifier"),
  addr_sid INT64 NOT NULL OPTIONS(description=" uniquely identifier each individual or organizatio address. it is a etl generated unique id of each address uniquly"),
  valid_from_date DATETIME NOT NULL OPTIONS(description=" date on which the record became valid. load date typically."),
  addr_type_code STRING OPTIONS(description=" captures address type of an business facility or individual person address."),
  valid_to_date DATETIME OPTIONS(description=" date on which the record was invalidated."),
  employee_num INT64 NOT NULL OPTIONS(description=" employee number of an employee associated with hr lawson company"),
  lawson_company_num INT64 NOT NULL OPTIONS(description=" the number that identifies a company.a company represents a business or legal entity of an organization"),
  process_level_code STRING NOT NULL OPTIONS(description=" unique process level code of an hr company value mainatined in this field."),
  delete_ind STRING OPTIONS(description=" the indicator is usually a y/ n (yes/no) designation of a record but can sometimes be other values.  this should not be 1/0 designation."),
  source_system_code STRING NOT NULL OPTIONS(description=" a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description=" datetime of update or load of this record to the enterprise data warehouse.")
)
PARTITION BY DATE(valid_from_date)
CLUSTER BY employee_sid, addr_sid
OPTIONS(
  description=" employee and his business working location and personal address captured"
);