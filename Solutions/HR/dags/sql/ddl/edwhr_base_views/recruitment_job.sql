create or replace view `{{ params.param_hr_base_views_dataset_name }}.recruitment_job`
AS SELECT
    recruitment_job.recruitment_job_sid,
    recruitment_job.valid_from_date,
    recruitment_job.valid_to_date,
    recruitment_job.recruitment_job_num,
    recruitment_job.job_title_name,
    recruitment_job.job_grade_code,
    recruitment_job.job_schedule_id,
    recruitment_job.overtime_status_id,
    recruitment_job.primary_facility_location_num,
    recruitment_job.recruiter_user_sid,
    recruitment_job.recruitment_job_parameter_sid,
    recruitment_job.recruitment_position_sid,
    recruitment_job.fte_pct,
    recruitment_job.source_system_code,
    recruitment_job.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.recruitment_job;