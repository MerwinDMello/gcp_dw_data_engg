/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.recruitment_job AS SELECT
      a.recruitment_job_sid,
      a.valid_from_date,
      a.valid_to_date,
      a.recruitment_job_num,
      a.job_title_name,
      a.job_grade_code,
      a.job_schedule_id,
      a.overtime_status_id,
      a.primary_facility_location_num,
      a.recruiter_user_sid,
      a.recruitment_job_parameter_sid,
      a.recruitment_position_sid,
      a.fte_pct,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS a
  ;

