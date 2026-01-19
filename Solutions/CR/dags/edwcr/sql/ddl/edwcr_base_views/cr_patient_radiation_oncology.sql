CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cr_patient_radiation_oncology
   OPTIONS(description='This table contains radiation oncology data for the patient')
  AS SELECT
      cr_patient_radiation_oncology.radiation_id,
      cr_patient_radiation_oncology.treatment_id,
      cr_patient_radiation_oncology.radiation_type_id,
      cr_patient_radiation_oncology.radiation_hospital_id,
      cr_patient_radiation_oncology.radiation_treatment_start_date,
      cr_patient_radiation_oncology.radiation_treatment_end_date,
      cr_patient_radiation_oncology.source_system_code,
      cr_patient_radiation_oncology.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cr_patient_radiation_oncology
  ;
