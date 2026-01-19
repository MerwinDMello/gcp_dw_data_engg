/***************************************************************************************
   Security   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_lawson_location_hospital_category AS SELECT
      ref_lawson_location_hospital_category.lawson_location_code,
      ref_lawson_location_hospital_category.hospital_category_code_year,
      ref_lawson_location_hospital_category.hospital_category_code,
      ref_lawson_location_hospital_category.source_system_code,
      ref_lawson_location_hospital_category.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_lawson_location_hospital_category
  ;

