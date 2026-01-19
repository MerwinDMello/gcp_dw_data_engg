CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cr_patient_history
   OPTIONS(description='Contains the patient and his/her family cancer history')
  AS SELECT
      cr_patient_history.cr_patient_id,
      cr_patient_history.tumor_id,
      cr_patient_history.smoked_history_id,
      cr_patient_history.tobacco_amt_text,
      cr_patient_history.years_tobacco_used_num_text,
      cr_patient_history.family_cancer_history_type_id,
      cr_patient_history.patient_cancer_history_type_id,
      cr_patient_history.source_system_code,
      cr_patient_history.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cr_patient_history
  ;
