CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cn_patient_surgery_stg (
cn_patient_surgery_sid INT64 NOT NULL
, surgeryside STRING
, surgeryfacility STRING
, surgery_type_id INT64
, core_record_type_id INT64
, med_spcl_physician_id INT64
, referring_physician_id INT64
, nav_patient_id NUMERIC(18,0)
, tumor_type_id INT64
, diagnosis_result_id INT64
, nav_diagnosis_id INT64
, navigator_id INT64
, coid STRING NOT NULL
, company_code STRING
, surgery_date DATE
, general_surgery_type_text STRING
, reconstructive_offered_ind STRING
, palliative_ind STRING
, comment_text STRING
, hashbite_ssk STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
