CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_input
   OPTIONS(description='Table contains the messages sent to the Cancer Id application')
  AS SELECT
      cancer_patient_id_input.message_control_id_text,
      cancer_patient_id_input.patient_type_status_sk,
      cancer_patient_id_input.coid,
      cancer_patient_id_input.company_code,
      cancer_patient_id_input.patient_dw_id,
      cancer_patient_id_input.pat_acct_num,
      cancer_patient_id_input.medical_record_num,
      cancer_patient_id_input.patient_market_urn,
      cancer_patient_id_input.message_type_code,
      cancer_patient_id_input.message_flag_code,
      cancer_patient_id_input.message_event_type_code,
      cancer_patient_id_input.message_origin_requested_date_time,
      cancer_patient_id_input.message_signed_observation_date_time,
      cancer_patient_id_input.message_created_date_time,
      cancer_patient_id_input.first_insert_date_time,
      cancer_patient_id_input.source_system_code,
      cancer_patient_id_input.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cancer_patient_id_input
  ;
