CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cancer_patient_id_output_wrk (
cancer_patient_id_output_sk INT64 NOT NULL
, message_control_id_text STRING NOT NULL
, coid STRING
, company_code STRING
, patient_dw_id NUMERIC(18,0)
, pat_acct_num NUMERIC(12,0)
, medical_record_num STRING
, patient_market_urn STRING
, last_name STRING
, first_name STRING
, birth_date DATE
, gender_code STRING
, social_security_num STRING
, address_line_1_text STRING
, address_line_2_text STRING
, city_name STRING
, state_code STRING
, zip_code STRING
, patient_type_code STRING
, message_type_code STRING
, message_event_type_code STRING
, message_flag_code STRING
, message_origin_requested_date_time DATETIME
, message_signed_observation_date_time DATETIME
, message_ingestion_date_time DATETIME
, message_created_date_time DATETIME
, document_type_id_text STRING
, document_type_text STRING
, document_type_coding_system_code STRING
, first_insert_date_time DATETIME
, icd_oncology_type_code STRING
, predicted_primary_icd_oncology_code STRING
, predicted_primary_icd_site_desc STRING
, suggested_primary_icd_oncology_code STRING
, suggested_primary_icd_site_desc STRING
, submitted_primary_icd_oncology_code STRING
, submitted_primary_icd_site_desc STRING
, transition_of_care_num INT64
, user_action_desc STRING
, user_action_criticality STRING
, user_action_date_time DATETIME
, user_3_4_id STRING
, report_assigned_date_time DATETIME
, attending_physician_full_name STRING
, pcp_full_name STRING
, pcp_phone_num STRING
, facility_menmonic_cs STRING
, network_mnemonic_cs STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
, model_predicted_sarcoma_ind STRING
, submitted_sarcoma_ind STRING
, suggested_sarcoma_ind STRING
, benign_brain_tumor_type_ind STRING
, met_to_brain_tumor_type_ind STRING
, other_tumor_type_ind STRING
)
  ;
