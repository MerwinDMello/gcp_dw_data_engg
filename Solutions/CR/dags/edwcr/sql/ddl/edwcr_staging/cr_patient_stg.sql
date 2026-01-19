CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cr_patient_stg (
patientid INT64 NOT NULL
, sex STRING
, race1 STRING
, vitalstatus STRING
, patientsystemid INT64
, facilityid INT64
, coid STRING
, birthdate DATE
, datelastcontact DATE
, firstname STRING
, middlename STRING
, lastname STRING
, patemail STRING
, accessionno STRING
, ptudf90 STRING
, medrecno STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
