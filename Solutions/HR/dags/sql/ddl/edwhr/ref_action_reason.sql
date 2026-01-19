create table if not exists `{{ params.param_hr_core_dataset_name }}.ref_action_reason`
(
  action_reason_text STRING NOT NULL OPTIONS(description="code describing the action reason."),
  lawson_company_num INT64 NOT NULL OPTIONS(description="the number that identifies a company.a company represents a business or legal entity of an organization"),
  action_reason_desc STRING OPTIONS(description="description of the action reason code."),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
CLUSTER BY action_reason_text, lawson_company_num
OPTIONS(
  description="contains descriptions for each of the action reasons."
);