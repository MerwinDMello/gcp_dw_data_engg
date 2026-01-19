CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.rad_onc_patient
   OPTIONS(description='Contains information of patients underwent Radiation Oncology')
  AS SELECT
      rad_onc_patient.patient_sk,
      rad_onc_patient.patient_address_sk,
      rad_onc_patient.site_sk,
      rad_onc_patient.source_patient_id,
      rad_onc_patient.medical_record_num,
      rad_onc_patient.patient_birth_date_time,
      rad_onc_patient.patient_first_name,
      rad_onc_patient.patient_middle_name,
      rad_onc_patient.patient_last_name,
      rad_onc_patient.patient_title_name,
      rad_onc_patient.patient_email_address_text,
      rad_onc_patient.patient_in_out_ind,
      rad_onc_patient.patient_death_ind,
      rad_onc_patient.patient_death_date,
      rad_onc_patient.patient_death_reason_text,
      rad_onc_patient.clinical_trial_ind,
      rad_onc_patient.patient_transportation_text,
      rad_onc_patient.patient_global_unique_id_text,
      rad_onc_patient.patient_room_number_text,
      rad_onc_patient.active_ind,
      rad_onc_patient.patient_language_text,
      rad_onc_patient.patient_notes_text,
      rad_onc_patient.log_id,
      rad_onc_patient.run_id,
      rad_onc_patient.history_user_name,
      rad_onc_patient.history_date_time,
      rad_onc_patient.source_system_code,
      rad_onc_patient.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.rad_onc_patient
  ;
