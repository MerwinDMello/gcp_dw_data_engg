/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.junc_submission_tracking_motive AS SELECT
      a.submission_tracking_sid,
      a.tracking_motive_id,
      a.valid_from_date,
      a.valid_to_date,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.junc_submission_tracking_motive AS a
  ;

