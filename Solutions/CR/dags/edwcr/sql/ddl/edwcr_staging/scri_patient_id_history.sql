CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.scri_patient_id_history (
msh_msg_control_id STRING
, coid STRING
, pid_pat_account_num STRING
, pid_medical_record_num STRING
, pid_medical_record_urn STRING
, msh_msg_type_message_code STRING
, message_flag STRING
, msh_msg_type_trigger_event STRING
, txa_origination_date_time STRING
, txa_transcription_date_time STRING
, message_created_date_time STRING
, patient_type_status STRING
, etl_insert_date_time STRING
)
  ;
