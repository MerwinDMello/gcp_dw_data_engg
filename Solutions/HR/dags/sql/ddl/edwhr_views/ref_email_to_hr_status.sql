/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_email_to_hr_status AS SELECT
      a.email_sent_status_id,
      a.email_sent_status_text,
      a.hr_status_desc,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_email_to_hr_status AS a
  ;

