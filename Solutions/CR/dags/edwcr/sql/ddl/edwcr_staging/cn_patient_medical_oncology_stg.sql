CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cn_patient_medical_oncology_stg (
cn_patient_medical_oncology_id INT64 NOT NULL
, treatment_type_id INT64
, medical_oncology_facility_id STRING
, core_record_type_id INT64
, med_spcl_physician_id INT64
, nav_patient_id NUMERIC(18,0)
, tumor_type_id INT64
, diagnosis_result_id INT64
, nav_diagnosis_id INT64
, navigator_id INT64
, coid STRING NOT NULL
, company_code STRING NOT NULL
, core_record_date DATE
, treatment_start_date DATE
, treatment_end_date DATE
, estimated_end_date DATE
, drug_name STRING
, dose_dense_chemo_ind STRING
, drug_dose_amt_text STRING
, drug_dose_measurement_text STRING
, drug_available_ind STRING
, drug_qty INT64
, cycle_num INT64
, cycle_frequency_text STRING
, medical_oncology_reason_text STRING
, terminated_ind STRING
, treatment_therapy_schedule_cd STRING
, comment_text STRING
, hashbite_ssk STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
