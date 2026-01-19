create table if not exists `{{ params.param_hr_core_dataset_name }}.job_code`
(
  job_code_sid INT64 NOT NULL OPTIONS(description="unique job code of an hr company."),
  valid_from_date DATETIME NOT NULL OPTIONS(description="date on which the record became valid. load date typically."),
  valid_to_date DATETIME OPTIONS(description="date on which the record was invalidated."),
  eff_from_date DATE OPTIONS(description="date on which record is initiated"),
  job_class_sid INT64 NOT NULL OPTIONS(description="unique user-defined job class code. it identifies job class of a hr company related job code."),
  hr_company_sid INT64 NOT NULL OPTIONS(description="contains the unique identifer for the hr company associated with the history record"),
  lawson_company_num INT64 NOT NULL OPTIONS(description="the number that identifies a company.a company represents a business or legal entity of an organization"),
  job_code STRING NOT NULL OPTIONS(description="unique job code of an hr company."),
  job_code_desc STRING NOT NULL OPTIONS(description="description of job code of an hr company"),
  active_dw_ind STRING OPTIONS(description="y/n character to indicate this record as active in the edw."),
  eeo_category_code STRING OPTIONS(description="categories defined by the eeo for compliance reporting that reflect the employees actual job duties and not necessarily their title."),
  eeo_code INT64 OPTIONS(description="codes are based on the position code of an employees primary position and are defined by the eeo for compliance reporting."),
  process_level_code STRING NOT NULL OPTIONS(description="unique process level code of an hr company value mainatined in this field."),
  security_key_text STRING NOT NULL OPTIONS(description="it is the key which has concatenation of lawson company code - process level code -department code for data access feature prospective. "),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
PARTITION BY DATE(valid_from_date)
CLUSTER BY job_code_sid
OPTIONS(
  description="this stores information about user-defined job codes. the lawson payroll system uses job codes to determine an employees rate of pay."
);