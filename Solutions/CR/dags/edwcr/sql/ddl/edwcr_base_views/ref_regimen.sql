CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_regimen
   OPTIONS(description='Contains the distinct list of regimen')
  AS SELECT
      ref_regimen.regimen_id,
      ref_regimen.regimen_name,
      ref_regimen.source_system_code,
      ref_regimen.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_regimen
  ;
