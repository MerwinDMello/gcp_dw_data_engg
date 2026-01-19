CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_medical_oncology
   OPTIONS(description='Contains the details behind the medical oncology treatment for a patient.')
  AS SELECT
      cn_patient_medical_oncology.cn_patient_medical_oncology_sid,
      cn_patient_medical_oncology.treatment_type_id,
      cn_patient_medical_oncology.medical_oncology_facility_id,
      cn_patient_medical_oncology.core_record_type_id,
      cn_patient_medical_oncology.med_spcl_physician_id,
      cn_patient_medical_oncology.nav_patient_id,
      cn_patient_medical_oncology.tumor_type_id,
      cn_patient_medical_oncology.diagnosis_result_id,
      cn_patient_medical_oncology.nav_diagnosis_id,
      cn_patient_medical_oncology.navigator_id,
      cn_patient_medical_oncology.coid,
      cn_patient_medical_oncology.company_code,
      cn_patient_medical_oncology.core_record_date,
      cn_patient_medical_oncology.treatment_start_date,
      cn_patient_medical_oncology.treatment_end_date,
      cn_patient_medical_oncology.estimated_end_date,
      cn_patient_medical_oncology.drug_name,
      cn_patient_medical_oncology.dose_dense_chemo_ind,
      cn_patient_medical_oncology.drug_dose_amt_text,
      cn_patient_medical_oncology.drug_dose_measurement_text,
      cn_patient_medical_oncology.drug_available_ind,
      cn_patient_medical_oncology.drug_qty,
      cn_patient_medical_oncology.cycle_num,
      cn_patient_medical_oncology.cycle_frequency_text,
      cn_patient_medical_oncology.medical_oncology_reason_text,
      cn_patient_medical_oncology.terminated_ind,
      cn_patient_medical_oncology.treatment_therapy_schedule_code,
      cn_patient_medical_oncology.comment_text,
      cn_patient_medical_oncology.hashbite_ssk,
      cn_patient_medical_oncology.source_system_code,
      cn_patient_medical_oncology.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_medical_oncology
  ;
