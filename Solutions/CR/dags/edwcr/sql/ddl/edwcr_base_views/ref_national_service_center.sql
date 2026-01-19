CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_national_service_center
   OPTIONS(description='This table contains National Service Center Codes and descriptions')
  AS SELECT
      ref_national_service_center.nsc_id,
      ref_national_service_center.nsc_code,
      ref_national_service_center.nsc_sub_code,
      ref_national_service_center.nsc_desc,
      ref_national_service_center.nsc_category_text,
      ref_national_service_center.nsc_sub_category_text,
      ref_national_service_center.source_system_code,
      ref_national_service_center.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_national_service_center
  ;
