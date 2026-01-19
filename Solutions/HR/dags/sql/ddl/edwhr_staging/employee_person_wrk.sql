CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.employee_person_wrk (
sk INT64
, hr_company_sid INT64
, home_phone_country_code STRING
, employee_num INT64
, employee_ssn STRING
, lawson_company_num INT64
, employee_first_name STRING
, employee_last_name STRING
, employee_middle_name STRING
, employee_home_phone_num STRING
, employee_work_phone_num STRING
, employee_sector_code STRING NOT NULL
, ethnic_origin_code STRING
, anniversary_date DATE
, gender_code STRING
, birth_date DATE
, email_text STRING
, veteran_ind STRING
, benefit_salary_amt NUMERIC(11,2)
, creation_date DATE
, eff_from_date DATE
, eff_to_date DATE
, active_dw_ind STRING
, process_level_code STRING NOT NULL
, source_system_code STRING
, handicap_id STRING
, badge_code STRING
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
