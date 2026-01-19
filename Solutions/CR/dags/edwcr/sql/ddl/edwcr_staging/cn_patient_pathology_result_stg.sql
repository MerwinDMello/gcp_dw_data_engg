CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cn_patient_pathology_result_stg (
cn_patient_pathology_res_sid INT64 NOT NULL
, pathology_result_type_id INT64
, core_record_type_id INT64
, nav_patient_id NUMERIC(18,0)
, tumor_type_id INT64
, diagnosis_result_id INT64
, nav_diagnosis_id INT64
, navigator_id INT64
, coid STRING NOT NULL
, company_code STRING NOT NULL
, result_value_text STRING
, hashbite_ssk STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
