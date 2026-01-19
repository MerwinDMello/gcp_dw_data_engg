/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.submission_tracking_status AS SELECT
      a.submission_tracking_sid,
      a.valid_from_date,
      a.valid_to_date,
      a.submission_status_id,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status AS a
  ;

