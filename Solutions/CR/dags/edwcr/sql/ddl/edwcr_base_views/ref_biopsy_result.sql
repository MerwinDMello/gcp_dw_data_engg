CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_biopsy_result
   OPTIONS(description='Contains a distinct list of biopsy results outcomes.')
  AS SELECT
      ref_biopsy_result.biopsy_result_id,
      ref_biopsy_result.biopsy_result_desc,
      ref_biopsy_result.source_system_code,
      ref_biopsy_result.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_biopsy_result
  ;
