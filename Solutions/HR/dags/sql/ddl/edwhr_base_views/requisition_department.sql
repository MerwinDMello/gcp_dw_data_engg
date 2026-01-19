create or replace view `{{ params.param_hr_base_views_dataset_name }}.requisition_department`
AS SELECT
    requisition_department.requisition_sid,
    requisition_department.valid_from_date,
    requisition_department.dept_sid,
    requisition_department.valid_to_date,
    requisition_department.dept_code,
    requisition_department.requisition_num,
    requisition_department.lawson_company_num,
    requisition_department.process_level_code,
    requisition_department.security_key_text,
    requisition_department.source_system_code,
    requisition_department.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.requisition_department;