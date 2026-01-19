-- ------------------------------------------------------------------------------
/***************************************************************************************
   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_process_level_target_hire AS SELECT
      ref_process_level_target_hire.process_level_sid,
      ref_process_level_target_hire.period_year_month_code,
      ref_process_level_target_hire.process_level_code,
      ref_process_level_target_hire.target_hire_num,
      ref_process_level_target_hire.source_system_code,
      ref_process_level_target_hire.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_process_level_target_hire
  ;

