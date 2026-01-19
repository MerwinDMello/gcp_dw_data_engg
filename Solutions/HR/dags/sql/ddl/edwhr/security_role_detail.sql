create table if not exists `{{ params.param_hr_core_dataset_name }}.security_role_detail`
(
  lawson_company_num INT64 NOT NULL OPTIONS(description="the number that identifies a company.a company represents a business or legal entity of an organization"),
  process_level_code STRING NOT NULL OPTIONS(description="unique process level code of an hr company value mainatined in this field."),
  access_role_code STRING NOT NULL OPTIONS(description="describes the role of the user."),
  span_code STRING NOT NULL OPTIONS(description="category of security access an employee is provisioned to."),
  dept_code STRING NOT NULL OPTIONS(description="unique department code of an hr company"),
  view_only_span_code STRING OPTIONS(description="a type of span code that allows the user to view data but not update it."),
  last_update_date DATE OPTIONS(description="last date a record was updated in lawson."),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
CLUSTER BY lawson_company_num, process_level_code, access_role_code, span_code
OPTIONS(
  description="this table builds the security access to tie to an hr position."
);