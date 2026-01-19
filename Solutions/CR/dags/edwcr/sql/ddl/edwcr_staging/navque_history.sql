CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.navque_history (
navque_history_id INT64 NOT NULL
, navque_action_id INT64 NOT NULL
, navque_reason_id INT64
, tumor_type_id INT64
, navigator_id INT64
, coid STRING
, company_code STRING
, message_control_id_text STRING
, message_date DATE
, navque_insert_date DATE
, navque_action_date DATE
, medical_record_num STRING
, patient_market_urn STRING
, network_mnemonic_cs STRING
, transition_of_care_score_num INT64
, navigated_patient_ind STRING
, message_source_flag STRING
, hashbite_ssk STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
