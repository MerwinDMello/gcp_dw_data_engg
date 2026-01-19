CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_sample_type
   OPTIONS(description='Contains the list of sample type for disease assessment source')
  AS SELECT
      ref_sample_type.sample_type_id,
      ref_sample_type.sample_type_name,
      ref_sample_type.source_system_code,
      ref_sample_type.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_sample_type
  ;
