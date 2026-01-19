CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_treatment_type
   OPTIONS(description='Contains the distinct list of patient treatments.')
  AS SELECT
      ref_treatment_type.treatment_type_id,
      ref_treatment_type.treatment_type_desc,
      ref_treatment_type.source_system_code,
      ref_treatment_type.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_treatment_type
  ;
