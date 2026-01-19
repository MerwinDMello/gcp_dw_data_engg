CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cn_patient_timeline_stg (
cn_patient_timeline_id INT64 NOT NULL
, nav_patient_id NUMERIC(18,0)
, tumor_type_id INT64
, navigator_id INT64
, coid STRING NOT NULL
, company_code STRING NOT NULL
, nav_referred_date DATE
, first_treatment_date DATE
, first_consult_date DATE
, first_imaging_date DATE
, first_medical_oncology_date DATE
, first_radiation_oncology_date DATE
, first_diagnosis_date DATE
, first_biopsy_date DATE
, first_surgery_consult_date DATE
, first_surgery_date DATE
, surv_care_plan_close_date DATE
, surv_care_plan_resolve_date DATE
, end_treatment_date DATE
, death_date DATE
, diag_first_trt_day_num INT64
, diag_first_trt_available_ind STRING
, hashbite_ssk STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
