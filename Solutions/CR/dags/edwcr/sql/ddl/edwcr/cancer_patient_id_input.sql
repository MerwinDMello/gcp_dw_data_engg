CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cancer_patient_id_input
(
  message_control_id_text STRING NOT NULL OPTIONS(description='Unique id for the message provided from EDW'),
  patient_type_status_sk INT64 OPTIONS(description='Identifier for status of patient type'),
  coid STRING OPTIONS(description='A five character code for each facility.'),
  company_code STRING OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  patient_dw_id NUMERIC(29) OPTIONS(description='An internal EDW identifier for each encounter.'),
  pat_acct_num NUMERIC(29) OPTIONS(description='Account Number for the message'),
  medical_record_num STRING OPTIONS(description='MRN associated to the message'),
  patient_market_urn STRING OPTIONS(description='URN associated to the message'),
  message_type_code STRING OPTIONS(description='The message type from the HL7 message.'),
  message_flag_code STRING OPTIONS(description='Identifies which type of segment the message corresponds to.'),
  message_event_type_code STRING OPTIONS(description='The triggering event from the HL7 message.'),
  message_origin_requested_date_time DATETIME OPTIONS(description='It will have TXA Origination Date Time from MDM message OR OBR requested Date Time from ORU Message '),
  message_signed_observation_date_time DATETIME OPTIONS(description='It will have TXA Transcription Date Time from MDM Message OR OBR Observation Date Time from ORU message '),
  message_created_date_time DATETIME OPTIONS(description='The datetime the message was created.'),
  first_insert_date_time DATETIME OPTIONS(description='Identifies when the message was first inserted.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY message_control_id_text
OPTIONS(
  description='Table contains the messages sent to the Cancer Id application'
);
