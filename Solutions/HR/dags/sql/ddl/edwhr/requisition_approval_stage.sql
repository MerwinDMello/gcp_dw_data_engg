create table if not exists `{{ params.param_hr_core_dataset_name }}.requisition_approval_stage`
(
  requisition_approval_type_code STRING NOT NULL OPTIONS(description="unique value of approval type like primary, final or secondary approval"),
  requisition_sid INT64 NOT NULL OPTIONS(description="unique id of an lawson generated requisition."),
  valid_from_date DATETIME NOT NULL OPTIONS(description="date on which the record became valid. load date typically."),
  valid_to_date DATETIME OPTIONS(description="date on which the record was invalidated."),
  approval_start_date DATE OPTIONS(description="contains the ending effective date for the requisition approval."),
  approver_employee_num INT64 NOT NULL OPTIONS(description="this field captures employee number of an employee who approves the requisition."),
  approval_desc STRING OPTIONS(description="describes the level of approver"),
  approver_position_title_desc STRING OPTIONS(description="contains the title of the position responsible for approving."),
  lawson_company_num INT64 NOT NULL OPTIONS(description="the number that identifies a company.a company represents a business or legal entity of an organization"),
  process_level_code STRING NOT NULL OPTIONS(description="unique process level code of an hr company value mainatined in this field."),
  active_dw_ind STRING NOT NULL OPTIONS(description="y/n character to indicate this record as active in the edw."),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
PARTITION BY DATE(valid_from_date)
CLUSTER BY requisition_approval_type_code, requisition_sid
OPTIONS(
  description="requisition approval stages captured in this table"
);