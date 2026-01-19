create table if not exists `{{ params.param_hr_core_dataset_name }}.hr_company`
(
  hr_company_sid INT64 NOT NULL OPTIONS(description="the number that identifies a hr company. hr company represents a business or legal                      entity of an organization as maintained in lawson. this column carries lawson defined unique hr company  value."),
  valid_from_date DATETIME NOT NULL OPTIONS(description="date on which the record became valid. load date typically."),
  valid_to_date DATETIME OPTIONS(description="date on which the record was invalidated."),
  company_code STRING NOT NULL OPTIONS(description="part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes."),
  lawson_company_num INT64 NOT NULL OPTIONS(description="unique four digit numeric value of lawson generated hr company maintained in this field"),
  company_name STRING NOT NULL OPTIONS(description="it captures descriptive name of hr company"),
  active_dw_ind STRING NOT NULL OPTIONS(description="y/n character to indicate this record as active in the edw."),
  security_key_text STRING NOT NULL OPTIONS(description="it is the key which has concatenation of lawson company code - process level code -department code for data access feature prospective. "),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
PARTITION BY DATE(valid_from_date)
CLUSTER BY hr_company_sid
OPTIONS(
  description="it maintains unique hr company details."
);