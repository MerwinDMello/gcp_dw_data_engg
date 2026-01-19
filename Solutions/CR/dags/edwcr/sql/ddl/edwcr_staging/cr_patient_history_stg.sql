CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cr_patient_history_stg (
patientid INT64 NOT NULL
, tumorid INT64 NOT NULL
, smokhist STRING
, amounttobacco STRING
, yrstobacco1 STRING
, famhxca STRING
, pthxca STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
