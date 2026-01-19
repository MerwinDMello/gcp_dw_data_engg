CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.junc_candidate_address_wrk
(
  candidate_sid INT64 NOT NULL,
  valid_from_date DATETIME NOT NULL,
  addr_sid INT64 NOT NULL,
  addr_type_code STRING NOT NULL,
  valid_to_date DATETIME,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);