CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_consultation
   OPTIONS(description='Contains the details associated with each consultation of a patient and physician.')
  AS SELECT
      cn_patient_consultation.cn_patient_consultation_sid,
      cn_patient_consultation.tumor_type_id,
      cn_patient_consultation.diagnosis_result_id,
      cn_patient_consultation.nav_diagnosis_id,
      cn_patient_consultation.navigator_id,
      cn_patient_consultation.consult_type_id,
      cn_patient_consultation.nav_patient_id,
      cn_patient_consultation.coid,
      cn_patient_consultation.company_code,
      cn_patient_consultation.med_spcl_physician_id,
      cn_patient_consultation.consult_other_type_text,
      cn_patient_consultation.consult_date,
      cn_patient_consultation.consult_phone_num,
      cn_patient_consultation.consult_notes_text,
      cn_patient_consultation.hashbite_ssk,
      cn_patient_consultation.source_system_code,
      cn_patient_consultation.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_consultation
  ;
