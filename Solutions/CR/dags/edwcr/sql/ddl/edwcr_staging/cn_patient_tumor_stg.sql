CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cn_patient_tumor_stg (
cn_patient_tumor_sid INT64
, tumorreferralsource STRING
, navigationstatus STRING
, endtreatmentphysician STRING
, endtreatmentlocation STRING
, treatment_end_reason_text STRING
, nav_patient_id NUMERIC(18,0)
, tumor_type_id INT64
, navigator_id INT64
, coid STRING
, electronic_folder_id_text STRING
, identification_period_text STRING
, referral_date DATE
, referring_physician_id INT64
, nav_end_reason_text STRING
, hashbite_ssk STRING
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
