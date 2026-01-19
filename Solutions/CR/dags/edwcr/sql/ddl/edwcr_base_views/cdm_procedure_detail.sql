CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cdm_procedure_detail
   OPTIONS(description='Contains all procedures associated with an encounter.')
  AS SELECT
      cdm_procedure_detail.patient_dw_id,
      cdm_procedure_detail.procedure_code,
      cdm_procedure_detail.procedure_type_code,
      cdm_procedure_detail.procedure_seq_num,
      cdm_procedure_detail.service_date,
      cdm_procedure_detail.procedure_code_desc,
      cdm_procedure_detail.procedure_type_code_desc,
      cdm_procedure_detail.coid,
      cdm_procedure_detail.company_code,
      cdm_procedure_detail.pat_acct_num,
      cdm_procedure_detail.procedure_rank_num,
      cdm_procedure_detail.facility_physician_num,
      cdm_procedure_detail.physician_npi,
      cdm_procedure_detail.physician_full_name,
      cdm_procedure_detail.physician_last_name,
      cdm_procedure_detail.physician_first_name,
      cdm_procedure_detail.source_system_code,
      cdm_procedure_detail.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cdm_procedure_detail
  ;
