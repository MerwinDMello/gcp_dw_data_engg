CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_surgery
   OPTIONS(description='Contains the surgical details associated with a patients treatment. Results from the treatment are stored in CN_Patient_Procedure_Pathology_Result.')
  AS SELECT
      cn_patient_surgery.cn_patient_surgery_sid,
      cn_patient_surgery.surgery_side_id,
      cn_patient_surgery.surgery_facility_id,
      cn_patient_surgery.surgery_type_id,
      cn_patient_surgery.core_record_type_id,
      cn_patient_surgery.med_spcl_physician_id,
      cn_patient_surgery.referring_physician_id,
      cn_patient_surgery.nav_patient_id,
      cn_patient_surgery.tumor_type_id,
      cn_patient_surgery.diagnosis_result_id,
      cn_patient_surgery.nav_diagnosis_id,
      cn_patient_surgery.navigator_id,
      cn_patient_surgery.coid,
      cn_patient_surgery.company_code,
      cn_patient_surgery.surgery_date,
      cn_patient_surgery.general_surgery_type_text,
      cn_patient_surgery.reconstructive_offered_ind,
      cn_patient_surgery.palliative_ind,
      cn_patient_surgery.comment_text,
      cn_patient_surgery.hashbite_ssk,
      cn_patient_surgery.source_system_code,
      cn_patient_surgery.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_surgery
  ;
