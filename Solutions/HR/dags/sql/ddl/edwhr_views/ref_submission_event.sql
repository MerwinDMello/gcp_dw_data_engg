/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_submission_event AS SELECT
      a.submission_event_id,
      a.submission_event_category_id,
      a.submission_event_code,
      a.submission_event_desc,
      a.submission_event_detail_desc,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_submission_event AS a
  ;

