CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.navque_patient_tumor_driver
   OPTIONS(description='This tables contain history information for Navigaton Queue')
  AS SELECT
      navque_patient_tumor_driver.navque_patient_tumor_driver_sk,
      navque_patient_tumor_driver.cancer_patient_driver_sk,
      navque_patient_tumor_driver.cancer_tumor_driver_sk,
      navque_patient_tumor_driver.navque_history_id,
      navque_patient_tumor_driver.navque_action_id,
      navque_patient_tumor_driver.navque_reason_id,
      navque_patient_tumor_driver.tumor_type_id,
      navque_patient_tumor_driver.navigator_id,
      navque_patient_tumor_driver.coid,
      navque_patient_tumor_driver.company_code,
      navque_patient_tumor_driver.message_control_id_text,
      navque_patient_tumor_driver.message_date,
      navque_patient_tumor_driver.navque_insert_date,
      navque_patient_tumor_driver.navque_action_date,
      navque_patient_tumor_driver.medical_record_num,
      navque_patient_tumor_driver.patient_market_urn,
      navque_patient_tumor_driver.network_mnemonic_cs,
      navque_patient_tumor_driver.transition_of_care_score_num,
      navque_patient_tumor_driver.navigated_patient_ind,
      navque_patient_tumor_driver.message_source_flag,
      navque_patient_tumor_driver.hashbite_ssk,
      navque_patient_tumor_driver.source_system_code,
      navque_patient_tumor_driver.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.navque_patient_tumor_driver
  ;
