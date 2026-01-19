create or replace view `{{ params.param_hr_base_views_dataset_name }}.job_code`
AS SELECT
    job_code.job_code_sid,
    job_code.valid_from_date,
    job_code.valid_to_date,
    job_code.eff_from_date,
    job_code.job_class_sid,
    job_code.hr_company_sid,
    job_code.lawson_company_num,
    job_code.job_code,
    job_code.job_code_desc,
    job_code.active_dw_ind,
    job_code.eeo_category_code,
    job_code.eeo_code,
    job_code.process_level_code,
    job_code.security_key_text,
    job_code.source_system_code,
    job_code.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.job_code;