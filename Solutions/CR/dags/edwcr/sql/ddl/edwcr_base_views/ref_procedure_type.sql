CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_procedure_type
   OPTIONS(description='Contains a distinct list of procedure types')
  AS SELECT
      ref_procedure_type.procedure_type_id,
      ref_procedure_type.procedure_type_desc,
      ref_procedure_type.procedure_sub_type_desc,
      ref_procedure_type.source_system_code,
      ref_procedure_type.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_procedure_type
  ;
