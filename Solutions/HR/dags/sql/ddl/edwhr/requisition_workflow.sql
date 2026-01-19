create table if not exists `{{ params.param_hr_core_dataset_name }}.requisition_workflow`
(
  requisition_sid INT64 NOT NULL OPTIONS(description="unique id of an lawson generated requisition."),
  workflow_seq_num INT64 NOT NULL OPTIONS(description="a sequence number for the order of the workflow generated in etl."),
  valid_from_date DATETIME NOT NULL OPTIONS(description="date on which the record became valid. load date typically."),
  valid_to_date DATETIME OPTIONS(description="date on which the record was invalidated."),
  workflow_workunit_num_text STRING OPTIONS(description="categories each activity number into a seperate group to help understand the workflow."),
  workflow_activity_num INT64 OPTIONS(description="the activity number captured by lawson for the workflow."),
  workflow_role_name STRING OPTIONS(description="the role of the user that approved the workflow."),
  workflow_task_name STRING OPTIONS(description="the operation that occured in the workflow."),
  start_date DATE OPTIONS(description="the date the workflow was assigned to the user."),
  start_time TIME OPTIONS(description="the time the workflow was assigned to the user."),
  end_date DATE OPTIONS(description="the date the workflow was approved or transitioned from the user."),
  end_time TIME OPTIONS(description="the time the workflow was approved or transitioned from the user."),
  workflow_user_id_code STRING OPTIONS(description="the 3-4 of the user that approved the workflow or the name of the system that transitioned the workflow."),
  lawson_company_num INT64 NOT NULL OPTIONS(description="the number that identifies a company.a company represents a business or legal entity of an organization"),
  process_level_code STRING NOT NULL OPTIONS(description="unique process level code of an hr company value mainatined in this field."),
  active_dw_ind STRING NOT NULL OPTIONS(description="y/n character to indicate this record as active in the edw."),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
CLUSTER BY requisition_sid
OPTIONS(
  description="contains the workflow of the requisition through the approval process."
);