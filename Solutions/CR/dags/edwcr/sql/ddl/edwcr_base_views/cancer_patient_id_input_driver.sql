CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_input_driver
   OPTIONS(description='Table contains the messages sent to the Cancer Id application and linked patient driver data')
  AS SELECT
      cancer_patient_id_input_driver.cancer_patient_id_input_driver_sk,
      cancer_patient_id_input_driver.cancer_patient_driver_sk,
      cancer_patient_id_input_driver.message_control_id_text,
      cancer_patient_id_input_driver.patient_type_status_sk,
      cancer_patient_id_input_driver.coid,
      cancer_patient_id_input_driver.company_code,
      cancer_patient_id_input_driver.patient_dw_id,
      cancer_patient_id_input_driver.pat_acct_num,
      cancer_patient_id_input_driver.medical_record_num,
      cancer_patient_id_input_driver.patient_market_urn,
      cancer_patient_id_input_driver.message_type_code,
      cancer_patient_id_input_driver.message_flag_code,
      cancer_patient_id_input_driver.message_event_type_code,
      cancer_patient_id_input_driver.message_origin_requested_date_time,
      cancer_patient_id_input_driver.message_signed_observation_date_time,
      cancer_patient_id_input_driver.message_created_date_time,
      cancer_patient_id_input_driver.first_insert_date_time,
      cancer_patient_id_input_driver.source_system_code,
      cancer_patient_id_input_driver.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cancer_patient_id_input_driver
  ;
