create or replace view `{{ params.param_hr_base_views_dataset_name }}.employee_detail`
AS SELECT
    employee_detail.employee_detail_code,
    employee_detail.employee_sid,
    employee_detail.applicant_type_id,
    employee_detail.valid_from_date,
    employee_detail.valid_to_date,
    employee_detail.detail_value_alphanumeric_text,
    employee_detail.detail_value_num,
    employee_detail.detail_value_date,
    employee_detail.employee_num,
    employee_detail.lawson_company_num,
    employee_detail.process_level_code,
    employee_detail.delete_ind,
    employee_detail.source_system_code,
    employee_detail.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.employee_detail;