CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cn_patient_biopsy_stg (
cn_patient_biopsy_sid INT64 NOT NULL
, core_record_type_id INT64
, med_spcl_physician_id INT64
, referring_physician_id INT64
, biopsy_type_id INT64
, biopsy_result_id INT64
, biopsyfacility STRING
, biopsysite STRING
, biopsyphysiciantype STRING
, nav_patient_id NUMERIC(18,0)
, tumor_type_id INT64
, diagnosis_result_id INT64
, nav_diagnosis_id INT64
, navigator_id INT64
, coid STRING NOT NULL
, company_code STRING NOT NULL
, biopsy_date DATE
, biopsy_clip_sw INT64
, biopsy_needle_sw INT64
, general_biopsy_type_text STRING
, comment_text STRING
, hashbite_ssk STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
