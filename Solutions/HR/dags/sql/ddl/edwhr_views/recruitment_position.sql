/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.recruitment_position AS SELECT
      a.recruitment_position_sid,
      a.valid_from_date,
      a.valid_to_date,
      a.recruitment_position_num,
      a.gsd_pct,
      a.incentive_payout_pct,
      a.incentive_plan_name,
      a.incentive_plan_potential_pct,
      a.special_program_name,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.recruitment_position AS a
  ;

