CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cancer_patient_id_input_wrk (
message_control_id_text STRING NOT NULL
, coid STRING
, company_code STRING
, patient_dw_id NUMERIC(18,0)
, pat_acct_num NUMERIC(12,0)
, medical_record_num STRING
, patient_market_urn STRING
, message_type_code STRING
, message_flag_code STRING
, message_event_type_code STRING
, message_origin_requested_date_time DATETIME
, message_signed_observation_date_time DATETIME
, message_created_date_time DATETIME
, first_insert_date_time DATETIME
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
, patient_type_status_sk INT64
)
  ;
