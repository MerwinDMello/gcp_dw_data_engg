CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_surgical_procedure_classification
   OPTIONS(description='Table maintained by the business that assigns a procedure site and category for each procedure code and procedure type code.')
  AS SELECT
      ref_surgical_procedure_classification.procedure_code,
      ref_surgical_procedure_classification.procedure_type_code,
      ref_surgical_procedure_classification.procedure_desc,
      ref_surgical_procedure_classification.procedure_site_name,
      ref_surgical_procedure_classification.procedure_category_name,
      ref_surgical_procedure_classification.source_system_code,
      ref_surgical_procedure_classification.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_surgical_procedure_classification
  ;
