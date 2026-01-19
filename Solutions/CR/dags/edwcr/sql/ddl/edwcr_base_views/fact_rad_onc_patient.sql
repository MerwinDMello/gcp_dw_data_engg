CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.fact_rad_onc_patient
   OPTIONS(description='Contains fact information of Radiation Oncology for a patient')
  AS SELECT
      fact_rad_onc_patient.fact_patient_sk,
      fact_rad_onc_patient.hospital_sk,
      fact_rad_onc_patient.patient_sk,
      fact_rad_onc_patient.patient_status_id,
      fact_rad_onc_patient.location_sk,
      fact_rad_onc_patient.race_id,
      fact_rad_onc_patient.gender_id,
      fact_rad_onc_patient.site_sk,
      fact_rad_onc_patient.source_fact_patient_id,
      fact_rad_onc_patient.creation_date_time,
      fact_rad_onc_patient.admission_date_time,
      fact_rad_onc_patient.discharge_date_time,
      fact_rad_onc_patient.log_id,
      fact_rad_onc_patient.run_id,
      fact_rad_onc_patient.source_system_code,
      fact_rad_onc_patient.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.fact_rad_onc_patient
  ;
