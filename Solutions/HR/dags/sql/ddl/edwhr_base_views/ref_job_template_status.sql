CREATE OR REPLACE VIEW {{params.param_hr_base_views_dataset_name}}.ref_job_template_status
AS SELECT
    ref_job_template_status.job_template_status_id,
    ref_job_template_status.job_template_status_desc,
    ref_job_template_status.source_system_code,
    ref_job_template_status.dw_last_update_date_time
  FROM
    {{params.param_hr_core_dataset_name}}.ref_job_template_status;