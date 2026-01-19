CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_transplant_type
   OPTIONS(description='Contains the distinct list of transplant type')
  AS SELECT
      ref_transplant_type.transplant_type_id,
      ref_transplant_type.transplant_type_name,
      ref_transplant_type.source_system_code,
      ref_transplant_type.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_transplant_type
  ;
