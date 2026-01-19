create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_requisition_status`
AS SELECT
    ref_requisition_status.requisition_status_id,
    ref_requisition_status.status_desc,
    ref_requisition_status.parent_requisition_status_id,
    ref_requisition_status.source_system_code,
    ref_requisition_status.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.ref_requisition_status;