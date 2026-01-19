CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cn_patient_radiation_oncology_stg (
cn_patient_rad_oncology_sid INT64 NOT NULL
, treatment_site_location_id STRING
, treatment_type_id INT64
, lung_lobe_location_id STRING
, radiation_oncology_facility_id STRING
, nav_patient_id NUMERIC(18,0)
, core_record_type_id INT64
, med_spcl_physician_id INT64
, tumor_type_id INT64
, diagnosis_result_id INT64
, nav_diagnosis_id INT64
, navigator_id INT64
, coid STRING NOT NULL
, company_code STRING NOT NULL
, core_record_date DATE
, treatment_start_date DATE
, treatment_end_date DATE
, treatment_fractions_num NUMERIC(6,2)
, elapse_ind STRING
, elapse_start_date DATE
, elapse_end_date DATE
, radiation_oncology_reason_text STRING
, palliative_ind STRING
, treatment_therapy_schedule_cd STRING
, comment_text STRING
, hashbite_ssk STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
