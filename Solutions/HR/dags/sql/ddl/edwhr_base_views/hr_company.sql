create or replace view `{{ params.param_hr_base_views_dataset_name }}.hr_company`
AS SELECT
    hr_company.hr_company_sid,
    hr_company.valid_from_date,
    hr_company.valid_to_date,
    hr_company.company_code,
    hr_company.lawson_company_num,
    hr_company.company_name,
    hr_company.active_dw_ind,
    hr_company.security_key_text,
    hr_company.source_system_code,
    hr_company.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.hr_company;