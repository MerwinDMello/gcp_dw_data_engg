CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_therapy_type
   OPTIONS(description='Contains a distinct list of therapy types.')
  AS SELECT
      ref_therapy_type.therapy_type_id,
      ref_therapy_type.therapy_type_desc,
      ref_therapy_type.source_system_code,
      ref_therapy_type.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_therapy_type
  ;
