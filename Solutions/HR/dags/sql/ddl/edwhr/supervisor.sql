create table if not exists `{{ params.param_hr_core_dataset_name }}.supervisor`
(
  supervisor_sid INT64 NOT NULL OPTIONS(description="etl generated unique numeric number assigned to each supervisor code"),
  valid_from_date DATETIME NOT NULL OPTIONS(description="date on which the record became valid. load date typically."),
  valid_to_date DATETIME OPTIONS(description="date on which the record was invalidated."),
  eff_from_date DATE NOT NULL OPTIONS(description="date on which record is initiated"),
  employee_sid INT64 OPTIONS(description="unique numeric number generated for unique combination of employee identifier and hr company identifier"),
  employee_num INT64 OPTIONS(description="this is an lawson employee number"),
  hr_company_sid INT64 NOT NULL OPTIONS(description="etl generated unique number for an unique hr company number"),
  reporting_supervisor_sid INT64 OPTIONS(description="unique identifier of an reporing supervisor code"),
  supervisor_code STRING NOT NULL OPTIONS(description="unique value of supervisor code"),
  supervisor_desc STRING OPTIONS(description="description of an supervisor"),
  officer_code STRING OPTIONS(description="describes whether the supervisor is an officer"),
  lawson_company_num INT64 NOT NULL OPTIONS(description="the number that identifies a company.a company represents a business or legal entity of an organization"),
  process_level_code STRING NOT NULL OPTIONS(description="unique process level code of an hr company value mainatined in this field."),
  active_dw_ind STRING NOT NULL OPTIONS(description="y/n character to indicate this record as active in the edw."),
  security_key_text STRING NOT NULL OPTIONS(description="it is the key which has concatenation of lawson company code - process level code -department code for data access feature prospective. "),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
PARTITION BY DATE(valid_from_date)
CLUSTER BY supervisor_sid
OPTIONS(
  description="contains the supervisor code and description. "
);