CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_biopsy_type
   OPTIONS(description='Contains a distinct list of biopsy types.')
  AS SELECT
      ref_biopsy_type.biopsy_type_id,
      ref_biopsy_type.biopsy_type_desc,
      ref_biopsy_type.source_system_code,
      ref_biopsy_type.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_biopsy_type
  ;
