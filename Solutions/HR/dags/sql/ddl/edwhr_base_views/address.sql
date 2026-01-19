CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.address
AS SELECT
    address.addr_sid,
    address.addr_type_code,
    address.addr_line_1_text,
    address.addr_line_2_text,
    address.addr_line_3_text,
    address.addr_line_4_text,
    address.city_name,
    address.zip_code,
    address.county_name,
    address.country_code,
    address.state_code,
    address.location_code,
    address.source_system_code,
    address.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.address;