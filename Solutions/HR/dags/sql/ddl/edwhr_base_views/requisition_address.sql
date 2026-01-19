create or replace view `{{ params.param_hr_base_views_dataset_name }}.requisition_address`
AS SELECT
requisition_address.requisition_sid,
requisition_address.valid_from_date,
requisition_address.addr_sid,
requisition_address.valid_to_date,
requisition_address.requisition_num,
requisition_address.lawson_company_num,
requisition_address.process_level_code,
requisition_address.source_system_code,
requisition_address.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.requisition_address;