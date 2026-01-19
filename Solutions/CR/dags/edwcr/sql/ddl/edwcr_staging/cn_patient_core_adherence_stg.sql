CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cn_patient_core_adherence_stg (
cn_patient_core_adherence_sid INT64 NOT NULL
, core_adherence_measure_id INT64
, nav_patient_id NUMERIC(18,0)
, tumor_type_id INT64
, diagnosis_result_id INT64
, nav_diagnosis_id INT64
, navigator_id INT64
, coid STRING NOT NULL
, company_code STRING NOT NULL
, core_adherence_measure_text STRING
, hashbite_ssk STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
