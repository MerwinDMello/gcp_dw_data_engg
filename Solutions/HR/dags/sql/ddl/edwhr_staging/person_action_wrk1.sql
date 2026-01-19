create table if not exists `{{ params.param_hr_stage_dataset_name }}.person_action_wrk1`
(
  action_nbr INT64 NOT NULL,
  action_code STRING NOT NULL,
  effect_date DATE NOT NULL,
  employee INT64 NOT NULL,
  company INT64 NOT NULL,
  action_type STRING,
  ant_end_date DATE,
  requisition INT64,
  applicant INT64,
  date_stamp DATE,
  process_level STRING,
  reason_01 STRING,
  user_id STRING,
  source_system_code STRING NOT NULL
)
