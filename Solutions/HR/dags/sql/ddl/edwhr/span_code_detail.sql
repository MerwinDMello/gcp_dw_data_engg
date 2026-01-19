create table if not exists `{{ params.param_hr_core_dataset_name }}.span_code_detail`
(
  span_code STRING NOT NULL OPTIONS(description="category of security access an employee is provisioned to."),
  system_code STRING NOT NULL OPTIONS(description="this code designates the type of system (billing, payroll, etc.)"),
  lawson_company_num INT64 NOT NULL OPTIONS(description="the number that identifies a company.a company represents a business or legal entity of an organization"),
  process_level_code STRING NOT NULL OPTIONS(description="unique process level code of an hr company value mainatined in this field."),
  unit_num STRING NOT NULL OPTIONS(description="this field maintains unit number of an hr company"),
  coid STRING OPTIONS(description="the company identifier which uniquely identifies a facility to corporate and all other facilities."),
  company_code STRING OPTIONS(description="part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes."),
  last_update_date DATE OPTIONS(description="last date a record was updated in lawson."),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
CLUSTER BY span_code, system_code, lawson_company_num, process_level_code
OPTIONS(
  description="this table contains details surrounding each span code."
);