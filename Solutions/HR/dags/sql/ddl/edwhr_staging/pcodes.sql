create table if not exists `{{ params.param_hr_stage_dataset_name }}.pcodes`
(
  code STRING NOT NULL,
  type STRING NOT NULL,
  active_flag STRING,
  description STRING,
  educ_level INT64,
  hipaa_reason STRING,
  hipaa_rel_code INT64,
  pay_work_flag INT64,
  recruit_avail STRING,
  r_count INT64,
  web_available STRING,
  web_avail_supv STRING,
  workflow_flag STRING,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
