CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cr_patient_medical_oncology
   OPTIONS(description='This table contains medical oncology data for the patient')
  AS SELECT
      cr_patient_medical_oncology.drug_id,
      cr_patient_medical_oncology.treatment_id,
      cr_patient_medical_oncology.cycle_id,
      cr_patient_medical_oncology.drug_route_id,
      cr_patient_medical_oncology.drug_dose_unit_id,
      cr_patient_medical_oncology.drug_hospital_id,
      cr_patient_medical_oncology.nsc_id,
      cr_patient_medical_oncology.total_drug_dose_amt,
      cr_patient_medical_oncology.drug_days_given_num_text,
      cr_patient_medical_oncology.drug_frequency_num,
      cr_patient_medical_oncology.treatment_start_date,
      cr_patient_medical_oncology.treatment_end_date,
      cr_patient_medical_oncology.cycle_num_text,
      cr_patient_medical_oncology.source_system_code,
      cr_patient_medical_oncology.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cr_patient_medical_oncology
  ;
