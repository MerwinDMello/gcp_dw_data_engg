CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.fact_rad_onc_treatment_history
(
  fact_treatment_history_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for fact data for treatment history in EDW'),
  patient_course_sk INT64 OPTIONS(description='Unique surrogate key generated for each patient course in EDW'),
  patient_plan_sk INT64 OPTIONS(description='Unique surrogate key generated for each plan in EDW'),
  patient_sk INT64 OPTIONS(description='Unique surrogate key generated for each patient in EDW'),
  treatment_intent_type_id INT64 OPTIONS(description='Identifier for treatment intent type'),
  clinical_status_id INT64 OPTIONS(description='Identifier for clinical status'),
  plan_status_id INT64 OPTIONS(description='Identifier for plan status'),
  field_technique_id INT64 OPTIONS(description='Identifier for field technique'),
  technique_id INT64 OPTIONS(description='Identifier for technique'),
  technique_label_id INT64 OPTIONS(description='Identifier for technique label'),
  technique_delivery_type_id INT64 OPTIONS(description='Identifier for technique delivery type'),
  site_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for a site in EDW'),
  source_fact_treatment_history_id INT64 NOT NULL OPTIONS(description='Unique identifier for treatment hsitory data in Radiation Oncology in source'),
  completion_date_time DATETIME OPTIONS(description='Date time for completion'),
  first_treatment_date_time DATETIME OPTIONS(description='Date time for first treatment'),
  last_treatment_date_time DATETIME OPTIONS(description='Date time for last treatment'),
  status_date_time DATETIME OPTIONS(description='Date time for status'),
  active_ind STRING OPTIONS(description='Indicates if record is active or not'),
  planned_dose_rate_num INT64 OPTIONS(description='Number for planned dose rate'),
  course_dose_delivered_amt BIGNUMERIC OPTIONS(description='Amount of dose delivered for course'),
  course_dose_planned_amt BIGNUMERIC OPTIONS(description='Amount of dose planned for course'),
  course_dose_remaining_amt BIGNUMERIC OPTIONS(description='Amount of dose remaining for course'),
  other_course_dose_delivered_amt BIGNUMERIC OPTIONS(description='Amount of other dose delivered for course'),
  dose_correction_amt BIGNUMERIC OPTIONS(description='Amount for dose correction'),
  total_dose_limit_amt BIGNUMERIC OPTIONS(description='Amount of total dose limit'),
  daily_dose_limit_amt BIGNUMERIC OPTIONS(description='Amount of daily dose limit'),
  session_dose_limit_amt BIGNUMERIC OPTIONS(description='Amount of session dose limit'),
  primary_ind STRING OPTIONS(description='Indicates if this is the primary record'),
  log_id INT64 OPTIONS(description='Unique identifier for the log'),
  run_id INT64 OPTIONS(description='Identifier for last run'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY site_sk, source_fact_treatment_history_id
OPTIONS(
  description='Contains fact information of Radiation Oncology for treatment history'
);
