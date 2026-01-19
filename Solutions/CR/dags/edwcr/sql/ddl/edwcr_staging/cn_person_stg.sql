CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cn_person_stg (
nav_patient_id NUMERIC(18,0) NOT NULL
, birth_date DATE
, first_name STRING
, last_name STRING
, middle_name STRING
, perferred_name STRING
, gender_code STRING
, preferred_langauage_text STRING
, death_date DATE
, patient_email_text STRING
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
