create or replace view `{{ params.param_hr_base_views_dataset_name }}.junc_employee_status`
AS SELECT
    junc_employee_status.employee_sid,
    junc_employee_status.status_sid,
    junc_employee_status.valid_from_date,
    junc_employee_status.status_type_code,
    junc_employee_status.valid_to_date,
    junc_employee_status.employee_num,
    junc_employee_status.status_code,
    junc_employee_status.lawson_company_num,
    junc_employee_status.process_level_code,
    junc_employee_status.delete_ind,
    junc_employee_status.source_system_code,
    junc_employee_status.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.junc_employee_status;