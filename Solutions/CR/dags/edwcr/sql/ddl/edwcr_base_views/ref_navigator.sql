CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_navigator
   OPTIONS(description='Contains the details around the navigator.')
  AS SELECT
      ref_navigator.navigator_id,
      ref_navigator.navigator_name,
      ref_navigator.navigator_3_4_id,
      ref_navigator.source_system_code,
      ref_navigator.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_navigator
  ;
