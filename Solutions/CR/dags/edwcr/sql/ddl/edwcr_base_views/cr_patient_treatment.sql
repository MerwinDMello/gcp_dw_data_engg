CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cr_patient_treatment
   OPTIONS(description='This table contains treatment information for the patient')
  AS SELECT
      cr_patient_treatment.treatment_id,
      cr_patient_treatment.tumor_id,
      cr_patient_treatment.treatment_hospital_id,
      cr_patient_treatment.treatment_type_id,
      cr_patient_treatment.surgical_site_id,
      cr_patient_treatment.surgical_margin_result_id,
      cr_patient_treatment.treatment_type_group_id,
      cr_patient_treatment.clinical_trial_start_date,
      cr_patient_treatment.treatment_start_date,
      cr_patient_treatment.clinical_trial_text,
      cr_patient_treatment.comment_text,
      cr_patient_treatment.treatment_performing_physician_code,
      cr_patient_treatment.source_system_code,
      cr_patient_treatment.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cr_patient_treatment
  ;
