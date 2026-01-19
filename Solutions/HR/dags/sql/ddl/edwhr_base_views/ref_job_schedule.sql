create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_job_schedule`
AS SELECT
    ref_job_schedule.job_schedule_id,
    ref_job_schedule.active_sw,
    ref_job_schedule.job_schedule_code,
    ref_job_schedule.job_schedule_desc,
    ref_job_schedule.job_schedule_seq_num,
    ref_job_schedule.source_system_code,
    ref_job_schedule.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.ref_job_schedule;