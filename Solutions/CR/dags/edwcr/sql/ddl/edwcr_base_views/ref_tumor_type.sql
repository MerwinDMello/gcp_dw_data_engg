CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_tumor_type
   OPTIONS(description='Reference for the type of tumor')
  AS SELECT
      ref_tumor_type.tumor_type_id,
      ref_tumor_type.tumor_type_desc,
      ref_tumor_type.navigation_tumor_code_id,
      ref_tumor_type.tumor_type_group_name,
      ref_tumor_type.source_system_code,
      ref_tumor_type.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_tumor_type
  ;
