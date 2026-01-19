CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.candidate_person_wrk
(
candidate_sid INT64 NOT NULL,
  valid_from_date DATETIME NOT NULL,
  first_name STRING,
  middle_name STRING,
  last_name STRING,
  social_security_num STRING,
  email_address STRING,
  maiden_name STRING,
  driver_license_num STRING,
  driver_license_state_code STRING,
  birth_date DATE,
  valid_to_date DATETIME,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
;
