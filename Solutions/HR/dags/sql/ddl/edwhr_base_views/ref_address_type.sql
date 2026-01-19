create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_address_type`
AS select 
addr_type_code
, addr_type_desc
, source_system_code
, dw_last_update_date_time
 FROM
    {{ params.param_hr_core_dataset_name }}.ref_address_type;