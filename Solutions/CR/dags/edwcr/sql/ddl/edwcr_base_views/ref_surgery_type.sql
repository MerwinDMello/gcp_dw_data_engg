CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_surgery_type
   OPTIONS(description='Contains a distinct list of surgery types.')
  AS SELECT
      ref_surgery_type.surgery_type_id,
      ref_surgery_type.surgery_type_desc,
      ref_surgery_type.source_system_code,
      ref_surgery_type.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_surgery_type
  ;
