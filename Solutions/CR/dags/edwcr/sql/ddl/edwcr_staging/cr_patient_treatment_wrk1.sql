CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cr_patient_treatment_wrk1 (
treatmentid INT64
, tumorid INT64
, treatmenthospid STRING
, rxcode STRING
, description STRING
, groupid STRING
, othercode STRING
, surgmarg STRING
, rxtype STRING
, dtprotocol DATE
, dtrxstart DATE
, prottitle STRING
, rxtxt STRING
, rxmd1 STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
