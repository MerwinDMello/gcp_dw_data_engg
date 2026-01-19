create or replace view `{{ params.param_hr_base_views_dataset_name }}.employee_requisition`
AS SELECT
employee_requisition.employee_sid,
employee_requisition.requisition_sid,
employee_requisition.action_type_code,
employee_requisition.eff_from_date,
employee_requisition.valid_from_date,
employee_requisition.valid_to_date,
employee_requisition.action_code,
employee_requisition.user_id_text,
employee_requisition.work_unit_num,
employee_requisition.lawson_company_num,
employee_requisition.process_level_code,
employee_requisition.requisition_num,
employee_requisition.employee_num,
employee_requisition.delete_ind,
employee_requisition.source_system_code,
employee_requisition.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.employee_requisition;