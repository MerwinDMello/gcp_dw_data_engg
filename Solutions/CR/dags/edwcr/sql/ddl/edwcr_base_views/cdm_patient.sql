CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cdm_patient
   OPTIONS(description='Contains patient information')
  AS SELECT
      cdm_patient.role_plyr_sk,
      cdm_patient.empi_text,
      cdm_patient.patient_race_code_sk,
      cdm_patient.patient_race_desc,
      cdm_patient.address_line_1_text,
      cdm_patient.address_line_2_text,
      cdm_patient.city_name,
      cdm_patient.state_code,
      cdm_patient.zip_code,
      cdm_patient.home_phone_num,
      cdm_patient.business_phone_num,
      cdm_patient.mobile_phone_num,
      cdm_patient.birth_date_time,
      cdm_patient.death_date_time,
      cdm_patient.first_name,
      cdm_patient.middle_name,
      cdm_patient.last_name,
      cdm_patient.gender_code,
      cdm_patient.patient_email_text,
      cdm_patient.vital_status_id,
      cdm_patient.source_system_code,
      cdm_patient.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cdm_patient
  ;
