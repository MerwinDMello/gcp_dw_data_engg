create table if not exists `{{ params.param_hr_core_dataset_name }}.applicant`
(
  applicant_sid INT64 NOT NULL OPTIONS(description="it  is the unique system generated identified for each applicant and lawson company combination"),
  valid_from_date DATETIME NOT NULL OPTIONS(description="date on which the record became valid. load date typically."),
  valid_to_date DATETIME OPTIONS(description="date on which the record was invalidated."),
  applicant_num INT64 OPTIONS(description="unique applicant number generated in job portal for application submitted"),
  lawson_company_num INT64 NOT NULL OPTIONS(description="it is the lawson company number of an hr company"),
  process_level_code STRING NOT NULL OPTIONS(description="it is process level code of an hr company"),
  employee_num INT64 OPTIONS(description="it is the unique number of an employee"),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
PARTITION BY DATE(valid_from_date)
CLUSTER BY applicant_sid
OPTIONS(
  description="it captures unique list of applicant who has offered for an job"
);