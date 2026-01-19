CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cr_patient_treatment_stg1 (
treatmentid INT64
, rxcode STRING
, description STRING
, groupid INT64
)
  ;
