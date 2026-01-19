create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_employee_detail`
AS SELECT
ref_employee_detail.employee_detail_code,
ref_employee_detail.employee_detail_type_code,
ref_employee_detail.employee_detail_desc,
ref_employee_detail.source_system_code,
ref_employee_detail.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_employee_detail;