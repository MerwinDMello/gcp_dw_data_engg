CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.development_activities_report_reject
(
  employee_id STRING,
  activity_competency_name STRING,
  description STRING,
  catalog_activity_name STRING,
  catalog_activity_description STRING,
  priority STRING,
  status STRING,
  start_date DATE,
  end_date DATE,
  hours STRING,
  development_goals_notes STRING,
  development_activity_record_id STRING,
  dw_last_update_date_time DATETIME,
  reject_reason STRING,
  reject_stg_tbl_nm STRING
);
