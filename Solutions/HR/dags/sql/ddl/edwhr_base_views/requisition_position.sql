create or replace view `{{ params.param_hr_base_views_dataset_name }}.requisition_position`
AS SELECT
    requisition_position.requisition_sid,
    requisition_position.valid_from_date,
    requisition_position.position_sid,
    requisition_position.valid_to_date,
    requisition_position.requisition_num,
    requisition_position.position_code,
    requisition_position.lawson_company_num,
    requisition_position.process_level_code,
    requisition_position.source_system_code,
    requisition_position.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.requisition_position;