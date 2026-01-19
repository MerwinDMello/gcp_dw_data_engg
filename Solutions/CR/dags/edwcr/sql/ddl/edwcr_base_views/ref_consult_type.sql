CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_consult_type
   OPTIONS(description='Contains a distinct list of consult types.')
  AS SELECT
      ref_consult_type.consult_type_id,
      ref_consult_type.consult_type_desc,
      ref_consult_type.source_system_code,
      ref_consult_type.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_consult_type
  ;
