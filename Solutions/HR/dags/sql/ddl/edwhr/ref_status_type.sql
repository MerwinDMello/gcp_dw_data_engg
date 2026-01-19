create table if not exists `{{ params.param_hr_core_dataset_name }}.ref_status_type`
(
  status_type_code STRING NOT NULL OPTIONS(description="it maintains unique list of employee status type codes like auxiliary status type code , employee status type code in this field."),
  status_type_desc STRING OPTIONS(description="this field captures decription for status type codes."),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
CLUSTER BY status_type_code
OPTIONS(
  description="it maintains unique list of status type codes for an employee and for requisition created in lawson system."
);