CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cdm_patient_history
   OPTIONS(description='Contains the information about the patient smoking status.')
  AS SELECT
      cdm_patient_history.clinical_patient_query_sid,
      cdm_patient_history.patient_dw_id,
      cdm_patient_history.coid,
      cdm_patient_history.company_code,
      cdm_patient_history.pat_acct_num,
      cdm_patient_history.smoking_status_id,
      cdm_patient_history.smoking_status_desc,
      cdm_patient_history.source_system_code,
      cdm_patient_history.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cdm_patient_history
  ;
