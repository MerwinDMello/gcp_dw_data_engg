CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cr_patient_staging_wrk (
cr_patient_staging_sid INT64 NOT NULL
, cr_patient_id INT64
, tumor_id INT64
, ajcc_stage_id INT64
, cancer_stage_classification_method_code STRING
, cancer_stage_type_code STRING
, cancer_stage_result_text STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
