create or replace view `{{ params.param_hr_base_views_dataset_name }}.recruitment_requisition_status`
AS SELECT
    recruitment_requisition_status.recruitment_requisition_sid,
    recruitment_requisition_status.valid_from_date,
    recruitment_requisition_status.valid_to_date,
    recruitment_requisition_status.requisition_status_id,
    recruitment_requisition_status.source_system_code,
    recruitment_requisition_status.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.recruitment_requisition_status;