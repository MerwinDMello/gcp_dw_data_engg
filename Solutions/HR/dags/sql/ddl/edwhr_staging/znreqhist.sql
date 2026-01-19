create table if not exists `{{ params.param_hr_stage_dataset_name }}.znreqhist`
(
  company INT64,
  requisition INT64,
  action_type STRING,
  employee INT64,
  effective_date DATE,
  user_id STRING,
  action_code STRING,
  wu_nbr NUMERIC,
  dw_last_update_date_time DATETIME
)
