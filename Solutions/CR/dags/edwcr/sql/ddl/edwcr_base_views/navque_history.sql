CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.navque_history
   OPTIONS(description='This tables contain history information for Navigaton Queue')
  AS SELECT
      navque_history.navque_history_id,
      navque_history.navque_action_id,
      navque_history.navque_reason_id,
      navque_history.tumor_type_id,
      navque_history.navigator_id,
      navque_history.coid,
      navque_history.company_code,
      navque_history.message_control_id_text,
      navque_history.message_date,
      navque_history.navque_insert_date,
      navque_history.navque_action_date,
      navque_history.medical_record_num,
      navque_history.patient_market_urn,
      navque_history.network_mnemonic_cs,
      navque_history.transition_of_care_score_num,
      navque_history.navigated_patient_ind,
      navque_history.message_source_flag,
      navque_history.hashbite_ssk,
      navque_history.source_system_code,
      navque_history.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.navque_history
  ;
