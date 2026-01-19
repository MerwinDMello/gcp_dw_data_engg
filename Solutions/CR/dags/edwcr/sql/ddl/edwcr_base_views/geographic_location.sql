CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.geographic_location AS SELECT
    geographic_location.zip_code,
    geographic_location.state_code,
    geographic_location.city_name,
    geographic_location.state_num,
    geographic_location.county_num,
    geographic_location.county_name,
    geographic_location.source_system_code
  FROM
    {{ params.param_pf_base_views_dataset_name }}.geographic_location
;
