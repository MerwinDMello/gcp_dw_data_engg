CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cr_patient_insurance_stg (
tumor_id INT64 NOT NULL
, patient_id INT64 NOT NULL
, primpayerdx STRING
, txtpaysource STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
