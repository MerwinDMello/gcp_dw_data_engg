/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_submission_status AS SELECT
      a.submission_status_id,
      a.active_sw,
      a.submission_state_id,
      a.submission_status_code,
      a.submission_status_name,
      a.submission_status_desc,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_submission_status AS a
  ;

