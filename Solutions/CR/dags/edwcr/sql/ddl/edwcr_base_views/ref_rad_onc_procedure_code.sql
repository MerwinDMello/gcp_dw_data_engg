CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_rad_onc_procedure_code
   OPTIONS(description='Contains information for radiation oncology procedure code')
  AS SELECT
      ref_rad_onc_procedure_code.procedure_code_sk,
      ref_rad_onc_procedure_code.treatment_type_sk,
      ref_rad_onc_procedure_code.site_sk,
      ref_rad_onc_procedure_code.source_procedure_code_id,
      ref_rad_onc_procedure_code.procedure_code,
      ref_rad_onc_procedure_code.procedure_short_desc,
      ref_rad_onc_procedure_code.procedure_long_desc,
      ref_rad_onc_procedure_code.active_ind,
      ref_rad_onc_procedure_code.log_id,
      ref_rad_onc_procedure_code.run_id,
      ref_rad_onc_procedure_code.source_system_code,
      ref_rad_onc_procedure_code.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_rad_onc_procedure_code
  ;
