CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.respondent_patient_detail_exception
(
  respondent_id NUMERIC(29) NOT NULL,
  survey_receive_date DATE NOT NULL,
  respondent_type_code STRING NOT NULL,
  survey_sid INT64 NOT NULL,
  company_code STRING,
  coid STRING,
  parent_coid STRING,
  pat_acct_num NUMERIC(29),
  patient_dw_id NUMERIC(29),
  discharge_date DATE,
  facility_claim_control_num STRING,
  exclusion_reason_code STRING,
  cms_exclusion_ind STRING,
  final_record_ind STRING,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
);