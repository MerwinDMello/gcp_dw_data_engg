CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_disease_assess_source
   OPTIONS(description='Contains the list of source for disease assessment')
  AS SELECT
      ref_disease_assess_source.disease_assess_source_id,
      ref_disease_assess_source.disease_assess_source_name,
      ref_disease_assess_source.source_system_code,
      ref_disease_assess_source.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_disease_assess_source
  ;
