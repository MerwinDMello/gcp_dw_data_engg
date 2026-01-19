create or replace view `{{ params.param_hr_base_views_dataset_name }}.job_class`
AS SELECT
    job_class.job_class_sid,
    job_class.valid_from_date,
    job_class.valid_to_date,
    job_class.hr_company_sid,
    job_class.lawson_company_num,
    job_class.job_class_code,
    job_class.job_class_desc,
    job_class.process_level_code,
    job_class.active_dw_ind,
    job_class.security_key_text,
    job_class.source_system_code,
    job_class.dw_last_update_date_time
  FROM
 {{ params.param_hr_core_dataset_name }}.job_class;