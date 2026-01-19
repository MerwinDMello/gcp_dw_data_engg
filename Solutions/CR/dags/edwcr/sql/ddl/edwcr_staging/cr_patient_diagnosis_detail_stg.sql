CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cr_patient_diagnosis_detail_stg (
tumor_id INT64 NOT NULL
, cr_patient_id INT64 NOT NULL
, histosubcode STRING
, laterality STRING
, histology STRING
, diagnosis_date DATE
, diagnose_age_num INT64
, first_diagnose_year_num INT64
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
