create table if not exists `{{ params.param_hr_core_dataset_name }}.department`
(
  dept_sid INT64 NOT NULL OPTIONS(description="unique etl generated numeric number for each department code of process level code associated with an hr company code."),
  valid_from_date DATETIME NOT NULL OPTIONS(description="date on which the record became valid. load date typically."),
  valid_to_date DATETIME OPTIONS(description="date on which the record was invalidated."),
  process_level_sid INT64 NOT NULL OPTIONS(description="unique code of an hr company facility or process level. this field is spaces for hr companyrecords"),
  dept_code STRING NOT NULL OPTIONS(description="unique department code of an hr company"),
  dept_name STRING NOT NULL OPTIONS(description="this value is the name of an department code"),
  lawson_company_num INT64 NOT NULL OPTIONS(description="the number that identifies a company.a company represents a business or legal entity of an organization"),
  process_level_code STRING NOT NULL OPTIONS(description="unique process level code of an hr company value mainatined in this field."),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  active_dw_ind STRING NOT NULL OPTIONS(description="y/n character to indicate this record as active in the edw."),
  security_key_text STRING NOT NULL OPTIONS(description="it is the key which has concatenation of lawson company code - process level code -department code for data access feature prospective. "),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
PARTITION BY DATE(valid_from_date)
CLUSTER BY dept_sid
OPTIONS(
  description="this tables stores information about the department in the organization structure .  departments are thelowest level for expensing and reporting in the lawson payroll system ."
);