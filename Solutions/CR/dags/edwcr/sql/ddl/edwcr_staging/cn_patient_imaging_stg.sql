CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cn_patient_imaging_stg (
cn_patient_imaging_sid INT64
, core_record_type_id INT64
, nav_patient_id NUMERIC(18,0)
, med_spcl_physician_id INT64
, tumor_type_id INT64
, diagnosis_result_id INT64
, nav_diagnosis_id INT64
, navigator_id INT64
, coid STRING
, imaging_type_id INT64
, imaging_date DATE
, imagemode STRING
, imagearea STRING
, imaging_location_text STRING
, imagecenter STRING
, birad_scale_code STRING
, comment_text STRING
, disease_status STRING
, treatment_status STRING
, other_image_type_text STRING
, initial_diagnosis_ind STRING
, disease_monitoring_ind STRING
, radiology_result_text STRING
, hashbite_ssk STRING
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
