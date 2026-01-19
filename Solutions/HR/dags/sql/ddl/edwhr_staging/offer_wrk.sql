CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.offer_wrk
(
  file_date DATE,
  offer_sid INT64 NOT NULL,
  valid_from_date DATETIME NOT NULL,
  valid_to_date DATETIME,
  offer_num INT64,
  submission_sid INT64,
  sequence_num INT64,
  offer_name STRING,
  accept_date DATE,
  start_date DATE,
  extend_date DATE,
  last_modified_date DATE,
  last_modified_time TIME,
  capture_date DATE,
  salary_amt BIGNUMERIC(53, 15),
  sign_on_bonus_amt BIGNUMERIC(53, 15),
  salary_pay_basis_amt BIGNUMERIC(53, 15),
  hours_per_day_num NUMERIC(31, 2),
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);