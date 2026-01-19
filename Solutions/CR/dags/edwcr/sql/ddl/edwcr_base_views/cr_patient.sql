CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cr_patient
   OPTIONS(description='Contains all the patients navigated by Sarah Cannon through Metriq')
  AS SELECT
      cr_patient.cr_patient_id,
      cr_patient.patient_gender_id,
      cr_patient.patient_race_id,
      cr_patient.vital_status_id,
      cr_patient.patient_system_id,
      cr_patient.coid,
      cr_patient.company_code,
      cr_patient.patient_birth_date,
      cr_patient.last_contact_date,
      cr_patient.patient_first_name,
      cr_patient.patient_middle_name,
      cr_patient.patient_last_name,
      cr_patient.patient_email_address_text,
      cr_patient.accession_num_code,
      cr_patient.patient_market_urn_text,
      cr_patient.medical_record_num,
      cr_patient.source_system_code,
      cr_patient.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cr_patient
  ;
