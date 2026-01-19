create table if not exists `{{ params.param_hr_core_dataset_name }}.status`
(
  status_sid INT64 NOT NULL OPTIONS(description="unique identifier generated for values of an status code"),
  valid_from_date DATETIME NOT NULL OPTIONS(description="date on which the record became valid. load date typically."),
  valid_to_date DATETIME OPTIONS(description="date on which the record was invalidated."),
  hr_company_sid INT64 NOT NULL OPTIONS(description="contains the unique identifer for the hr company associated with the history record"),
  lawson_company_num INT64 NOT NULL OPTIONS(description="the number that identifies a company.a company represents a business or legal entity of an organization"),
  status_type_code STRING NOT NULL OPTIONS(description="unique list of status type code"),
  status_code STRING NOT NULL OPTIONS(description="this represents the available code values for the status of the employee or applicant. "),
  status_desc STRING NOT NULL OPTIONS(description="this represents the description or name for available code values for the status of the employee or applicant."),
  active_dw_ind STRING NOT NULL OPTIONS(description="y/n character to indicate this record as active in the edw."),
  process_level_code STRING NOT NULL OPTIONS(description="unique process level code of an hr company value mainatined in this field."),
  security_key_text STRING NOT NULL OPTIONS(description="it is the key which has concatenation of lawson company code - process level code -department code for data access feature prospective. "),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
PARTITION BY DATE(valid_from_date)
CLUSTER BY status_sid
OPTIONS(
  description="this stores information for employee and applicant statuses as created in lawson personnel system and lawson human resource system "
);