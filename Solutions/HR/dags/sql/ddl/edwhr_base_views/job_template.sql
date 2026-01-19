CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.job_template AS SELECT
    job_template.job_template_sid,
    job_template.valid_from_date,
    job_template.job_template_num,
    job_template.base_job_template_num,
    job_template.recruitment_job_sid,
    job_template.job_template_status_id,
    job_template.valid_to_date,
    job_template.source_system_code,
    job_template.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.job_template
;