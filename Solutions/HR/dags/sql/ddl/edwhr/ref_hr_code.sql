create table if not exists `{{ params.param_hr_core_dataset_name }}.ref_hr_code`
(
  hr_code STRING NOT NULL OPTIONS(description="contains the code types with which valid values are used. values of 1-99 are numbers assigned to user fields that have valid values associated with them. "),
  hr_type_code STRING NOT NULL OPTIONS(description="contains the type of code."),
  hr_code_desc STRING OPTIONS(description="the description of the code."),
  active_ind STRING OPTIONS(description="y/n character to indicate this record as active in the edw."),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
CLUSTER BY hr_code, hr_type_code
OPTIONS(
  description="this table contains reference data for many different code types from lawson."
);