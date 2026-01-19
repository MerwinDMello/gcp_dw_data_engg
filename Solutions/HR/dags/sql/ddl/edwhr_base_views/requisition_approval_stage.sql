create or replace view `{{ params.param_hr_base_views_dataset_name }}.requisition_approval_stage`
AS SELECT
    requisition_approval_stage.requisition_approval_type_code,
    requisition_approval_stage.requisition_sid,
    requisition_approval_stage.valid_from_date,
    requisition_approval_stage.valid_to_date,
    requisition_approval_stage.approval_start_date,
    requisition_approval_stage.approver_employee_num,
    requisition_approval_stage.approval_desc,
    requisition_approval_stage.approver_position_title_desc,
    requisition_approval_stage.lawson_company_num,
    requisition_approval_stage.process_level_code,
    requisition_approval_stage.active_dw_ind,
    requisition_approval_stage.source_system_code,
    requisition_approval_stage.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.requisition_approval_stage;