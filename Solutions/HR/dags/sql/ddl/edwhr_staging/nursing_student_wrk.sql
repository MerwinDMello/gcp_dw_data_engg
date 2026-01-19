CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.nursing_student_wrk
(
  student_sid INT64 NOT NULL,
  valid_from_date DATETIME NOT NULL,
  valid_to_date DATETIME,
  student_num INT64 NOT NULL,
  student_ssn STRING,
  student_first_name STRING,
  student_last_name STRING,
  student_middle_name STRING,
  birth_date DATE,
  gender_code STRING,
  ethnic_origin_desc STRING,
  addr_sid INT64,
  pell_grant_eligibility_ind STRING,
  first_gen_college_grad_ind STRING,
  employee_num INT64,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME
);