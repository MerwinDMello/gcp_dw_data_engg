CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cr_patient_wrk (
cr_patient_id INT64 NOT NULL
, patient_gender_id INT64
, patient_race_id INT64
, vital_status_id INT64
, patient_system_id INT64
, coid STRING NOT NULL
, company_code STRING NOT NULL
, patient_birth_date DATE
, last_contact_date DATE
, patient_first_name STRING
, patient_middle_name STRING
, patient_last_name STRING
, patient_email_address_text STRING
, accession_num_code STRING
, patient_market_urn_text STRING
, medical_record_num STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
