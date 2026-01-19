create table if not exists `{{ params.param_hr_stage_dataset_name }}.emstatus`
(
  emp_status STRING NOT NULL,
  company INT64 NOT NULL,
  active_flag STRING,
  date_stamp DATE,
  description STRING,
  emp_app INT64,
  hipaa_emp_code STRING,
  pay_status STRING,
  rel_status STRING,
  rel_to_org STRING,
  r_count INT64,
  time_stamp TIME,
  user_id STRING,
  work_type STRING,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
