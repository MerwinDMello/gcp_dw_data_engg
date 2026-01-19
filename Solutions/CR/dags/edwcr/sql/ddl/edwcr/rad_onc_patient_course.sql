CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.rad_onc_patient_course
(
  patient_course_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for each patient course in EDW'),
  patient_sk INT64 OPTIONS(description='Unique surrogate key for patient'),
  clinical_status_id INT64 OPTIONS(description='Identifier for clinical status'),
  treatment_intent_type_id INT64 OPTIONS(description='Identifier for type of treatment intent'),
  site_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for a site in EDW'),
  source_patient_course_id INT64 NOT NULL OPTIONS(description='Unique identifier for a patient course in Radiation Oncology in source'),
  course_id_text STRING OPTIONS(description='Text for course identifier'),
  course_start_date_time DATETIME OPTIONS(description='Start date time for course'),
  course_session_planned_num INT64 OPTIONS(description='Number of sessions planned for course'),
  course_session_delivered_num INT64 OPTIONS(description='Number of sessions delivered for course'),
  course_session_remaining_num INT64 OPTIONS(description='Number of sessions remaining for course'),
  dose_delivered_amt BIGNUMERIC(48, 10) OPTIONS(description='Amount of dose delivered'),
  course_duration_num INT64 OPTIONS(description='Duration of the course'),
  comment_text STRING OPTIONS(description='Text for the patient course comment'),
  log_id INT64 OPTIONS(description='Unique identifier for the log'),
  run_id INT64 OPTIONS(description='Identifier for last run'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY site_sk, source_patient_course_id
OPTIONS(
  description='Contains information of patient courses in Radiation Oncology'
);
