create or replace view `{{ params.param_hr_base_views_dataset_name }}.requisition_status`
AS SELECT
    requisition_status.requisition_sid,
    requisition_status.valid_from_date,
    requisition_status.status_sid,
    requisition_status.valid_to_date,
    requisition_status.requisition_num,
    requisition_status.status_code,
    requisition_status.lawson_company_num,
    requisition_status.process_level_code,
    requisition_status.source_system_code,
    requisition_status.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.requisition_status;