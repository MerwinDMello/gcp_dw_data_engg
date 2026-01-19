create or replace view `{{ params.param_hr_base_views_dataset_name }}.bi_employee_detail`
AS SELECT
    bi_employee_detail.employee_sid,
    bi_employee_detail.employee_num,
    bi_employee_detail.employee_first_name,
    bi_employee_detail.employee_last_name,
    bi_employee_detail.employee_middle_name,
    bi_employee_detail.ethnic_origin_code,
    bi_employee_detail.gender_code,
    bi_employee_detail.adjusted_hire_date,
    bi_employee_detail.birth_date,
    bi_employee_detail.acute_experience_start_date,
    bi_employee_detail.lawson_company_num,
    bi_employee_detail.process_level_code,
    bi_employee_detail.source_system_code,
    bi_employee_detail.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.bi_employee_detail;