/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.job_template AS SELECT
      a.job_template_sid,
      a.valid_from_date,
      a.job_template_num,
      a.base_job_template_num,
      a.recruitment_job_sid,
      a.job_template_status_id,
      a.valid_to_date,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.job_template AS a
  ;

