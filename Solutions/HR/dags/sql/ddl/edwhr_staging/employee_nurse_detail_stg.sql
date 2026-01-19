CREATE OR REPLACE TABLE {{ params.param_hr_stage_dataset_name }}.employee_nurse_detail_stg (
report_date STRING
, license_id STRING
, ee_num STRING
, first_name STRING
, last_name STRING
, middle_name STRING
, address1 STRING
, address2 STRING
, city STRING
, state STRING
, zip STRING
, facility_name STRING
, facility_num STRING
, deparment_num STRING
, department_name STRING
, job_code STRING
, job_title STRING
, position_code STRING
, position_desc STRING
, job_start_date STRING
, original_start_date STRING
, type_cd STRING
, precheck_type STRING
, hca_type STRING
, license_num STRING
, license_state STRING
, expiration_date STRING
, ncsbn_num STRING
, hr_company_code STRING
, dw_last_update_date_time DATETIME
)
  ;
