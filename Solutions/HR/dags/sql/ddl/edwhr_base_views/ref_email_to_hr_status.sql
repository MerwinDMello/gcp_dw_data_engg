create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_email_to_hr_status`
AS SELECT
    ref_email_to_hr_status.email_sent_status_id,
    ref_email_to_hr_status.email_sent_status_text,
    ref_email_to_hr_status.hr_status_desc,
    ref_email_to_hr_status.source_system_code,
    ref_email_to_hr_status.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.ref_email_to_hr_status;