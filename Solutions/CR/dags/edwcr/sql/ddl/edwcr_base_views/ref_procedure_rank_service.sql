CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_procedure_rank_service
   OPTIONS(description='This table is created to list out the Rankings and their descriptions, as well as the Service and Sub-Service Lines.')
  AS SELECT
      ref_procedure_rank_service.dss_op_cpt_hier,
      ref_procedure_rank_service.eff_from_date,
      ref_procedure_rank_service.full_description_txt,
      ref_procedure_rank_service.short_description_txt,
      ref_procedure_rank_service.service_line_txt,
      ref_procedure_rank_service.sub_service_line_txt,
      ref_procedure_rank_service.eff_to_date,
      ref_procedure_rank_service.source_system_code,
      ref_procedure_rank_service.dw_last_update_date_time
    FROM
      {{ params.param_pf_base_views_dataset_name }}.ref_procedure_rank_service
  ;
