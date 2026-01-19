create table if not exists `{{ params.param_hr_core_dataset_name }}.ref_action_type`
(
  action_type_code STRING NOT NULL OPTIONS(description="it represents the various action type code values available for personnel action. valid values are as below a - hire an applicant, e - individual action, m - mass change, p - mass pay change, l - multiple positions."),
  action_type_desc STRING OPTIONS(description="description of action type code"),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
CLUSTER BY action_type_code
OPTIONS(
  description="unique list of action codes for an action performed on personnel employee level, organization level etc maintained in this table."
);