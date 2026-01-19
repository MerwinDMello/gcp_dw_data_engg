CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_pathway_var_reason
   OPTIONS(description='Contains a distinct list of pathway variance reason')
  AS SELECT
      ref_pathway_var_reason.pathway_var_reason_id,
      ref_pathway_var_reason.pathway_var_reason_type_desc,
      ref_pathway_var_reason.pathway_var_reason_sub_type_desc,
      ref_pathway_var_reason.source_system_code,
      ref_pathway_var_reason.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_pathway_var_reason
  ;
