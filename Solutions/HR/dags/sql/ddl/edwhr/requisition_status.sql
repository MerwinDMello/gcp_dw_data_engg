create table if not exists `{{ params.param_hr_core_dataset_name }}.requisition_status`
(
  requisition_sid INT64 NOT NULL OPTIONS(description="unique id of an lawson generated requisition."),
  valid_from_date DATETIME NOT NULL OPTIONS(description="date on which the record became valid. load date typically."),
  status_sid INT64 OPTIONS(description="contains the unique identifer for the hr company associated with the history record"),
  valid_to_date DATETIME OPTIONS(description="date on which the record was invalidated."),
  requisition_num INT64 NOT NULL OPTIONS(description="unique id of an lawson generated requisition."),
  status_code STRING NOT NULL OPTIONS(description="this represents the available code values for the status of the employee or applicant. "),
  lawson_company_num INT64 NOT NULL OPTIONS(description="the number that identifies a company.a company represents a business or legal entity of an organization"),
  process_level_code STRING NOT NULL OPTIONS(description="unique process level code of an hr company value mainatined in this field."),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
PARTITION BY DATE(valid_from_date)
CLUSTER BY requisition_sid
OPTIONS(
  description="requisition goes through various status. requisition entire status history is maintained"
);