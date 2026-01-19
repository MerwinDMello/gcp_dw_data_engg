/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.address AS SELECT
      a.addr_sid,
      a.addr_type_code,
      a.addr_line_1_text,
      a.addr_line_2_text,
      a.addr_line_3_text,
      a.addr_line_4_text,
      a.city_name,
      a.zip_code,
      a.county_name,
      a.country_code,
      a.state_code,
      a.location_code,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.address AS a
  ;

