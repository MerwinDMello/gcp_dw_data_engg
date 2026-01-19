CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.junc_employee_recruitment_user_wrk
(
 employee_sid INT64 NOT NULL,
  recruitment_user_sid INT64 NOT NULL,
  valid_from_date DATETIME NOT NULL,
  primary_facility_ind STRING,
  valid_to_date DATETIME,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
);