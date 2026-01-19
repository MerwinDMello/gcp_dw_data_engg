CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_result
   OPTIONS(description='Contains surgery, biopsy result, diagnosis and other oncology navigation result possibilities.')
  AS SELECT
      ref_result.nav_result_id,
      ref_result.nav_result_desc,
      ref_result.source_system_code,
      ref_result.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_result
  ;
