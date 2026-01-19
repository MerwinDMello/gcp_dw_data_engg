CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cr_patient_radiation_oncology_wrk (
radiation_id INT64 NOT NULL
, treatment_id INT64
, radiation_type_id INT64
, radiation_hospital_id INT64
, radiation_treatment_start_date DATE
, radiation_treatment_end_date DATE
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
