/***************************************************************************************
   B A S E   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.recruitment_position AS SELECT
    recruitment_position.recruitment_position_sid,
    recruitment_position.valid_from_date,
    recruitment_position.valid_to_date,
    recruitment_position.recruitment_position_num,
    recruitment_position.gsd_pct,
    recruitment_position.incentive_payout_pct,
    recruitment_position.incentive_plan_name,
    recruitment_position.incentive_plan_potential_pct,
    recruitment_position.special_program_name,
    recruitment_position.source_system_code,
    recruitment_position.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.recruitment_position
;