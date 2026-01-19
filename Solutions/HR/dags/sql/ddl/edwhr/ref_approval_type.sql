create table if not exists `{{ params.param_hr_core_dataset_name }}.ref_approval_type`
(
  approval_type_code STRING NOT NULL OPTIONS(description="unique list of approval type codes are maintained in this field. approval type codes are approver_1, approver_2, final_approver etc"),
  approval_type_desc STRING OPTIONS(description="description for approver type code is  maintained in this field"),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
CLUSTER BY approval_type_code
OPTIONS(
  description="it mainatins complete list of approval type like approver_1, approver_2, final_approver etc"
);