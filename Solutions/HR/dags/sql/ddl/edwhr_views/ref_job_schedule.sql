/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_job_schedule AS SELECT
      a.job_schedule_id,
      a.active_sw,
      a.job_schedule_code,
      a.job_schedule_desc,
      a.job_schedule_seq_num,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_job_schedule AS a
  ;

