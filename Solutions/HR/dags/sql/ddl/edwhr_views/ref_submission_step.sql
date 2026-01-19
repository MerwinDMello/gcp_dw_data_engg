/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_submission_step AS SELECT
      a.step_id,
      a.submission_state_id,
      a.active_sw,
      a.step_code,
      a.step_name,
      a.step_short_name,
      a.step_desc,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_submission_step AS a
  ;

