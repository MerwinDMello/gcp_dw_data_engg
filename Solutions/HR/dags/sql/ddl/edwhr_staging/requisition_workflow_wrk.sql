create table if not exists `{{ params.param_hr_stage_dataset_name }}.requisition_workflow_wrk`
(
  requisition_sid INT64,
  workflow_seq_num INT64,
  valid_from_date DATETIME,
  valid_to_date DATETIME,
  workflow_workunit_num_text STRING,
  workflow_activity_num INT64,
  workflow_role_name STRING,
  workflow_task_name STRING,
  start_date DATE,
  start_time TIME,
  end_date DATE,
  end_time TIME,
  workflow_user_id_code STRING,
  lawson_company_num INT64,
  process_level_code STRING,
  active_dw_ind STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
)
