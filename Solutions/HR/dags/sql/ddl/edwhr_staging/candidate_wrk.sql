CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.candidate_wrk
(
  file_date DATE,
  candidate_sid INT64 NOT NULL,
  valid_from_date DATETIME NOT NULL,
  candidate_num INT64,
  in_hiring_process_sw INT64,
  internal_candidate_sw INT64,
  referred_sw INT64,
  last_modified_date_time DATETIME,
  candidate_creation_date_time DATETIME,
  residence_location_num INT64,
  travel_preference_code STRING,
  relocation_preference_code STRING,
  valid_to_date DATETIME,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
)
;