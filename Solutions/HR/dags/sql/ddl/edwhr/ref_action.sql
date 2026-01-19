create table if not exists `{{ params.param_hr_core_dataset_name }}.ref_action`
(
  action_code STRING NOT NULL OPTIONS(description="unique list of action code associated with the personnel action are maintained in this field."),
  lawson_company_num INT64 NOT NULL OPTIONS(description="the number that identifies a company.a company represents a business or legal entity of an organization"),
  active_flag INT64 NOT NULL OPTIONS(description="numeric boolean to where 0 = false/no and 1 = true/yes"),
  action_desc STRING OPTIONS(description="description of action code mainatined in this field"),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
CLUSTER BY action_code, lawson_company_num, active_flag
OPTIONS(
  description="unique action code for an employee of an hr company maintained"
);