CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.recruitment_position_wrk
(
  recruitment_position_sid INT64,
  recruitment_position_num INT64,
  gsd_pct NUMERIC(31, 2),
  incentive_payout_pct NUMERIC(31, 2),
  incentive_plan_name STRING,
  incentive_plan_potential_pct NUMERIC(31, 2),
  special_program_name STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
)
;
