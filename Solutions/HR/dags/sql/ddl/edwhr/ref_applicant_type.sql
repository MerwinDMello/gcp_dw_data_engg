create table if not exists `{{ params.param_hr_core_dataset_name }}.ref_applicant_type`
(
  applicant_type_id INT64 NOT NULL OPTIONS(description="indicates if the individual is an employee or an applicant.lawson system maintains 0 and 1 for employee and applicant respectively and same values are maintained in this field."),
  applicant_type_desc STRING OPTIONS(description="indicates if the individual is an employee or an applicant. e.g. employee, applicant "),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
CLUSTER BY applicant_type_id
OPTIONS(
  description="this table maintains applicant and employee type codes."
);