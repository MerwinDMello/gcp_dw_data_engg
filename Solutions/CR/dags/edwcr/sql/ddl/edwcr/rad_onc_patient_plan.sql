CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.rad_onc_patient_plan
(
  patient_plan_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for each plan in EDW'),
  plan_purpose_sk INT64 OPTIONS(description='Identifier for plan purpose'),
  site_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for a site in EDW'),
  source_patient_plan_id INT64 NOT NULL OPTIONS(description='Unique identifier for a plan in Radiation Oncology in source'),
  patient_course_sk INT64 OPTIONS(description='Unique key for a course assigend to a patient'),
  plan_status_id INT64 OPTIONS(description='Identifier for plan status'),
  plan_unique_id_text STRING OPTIONS(description='Text for plan unique Identifier'),
  plan_creation_date_time DATETIME OPTIONS(description='Date time plan was created'),
  treatment_start_date_time DATETIME OPTIONS(description='Date time treatment was started'),
  treatment_end_date_time DATETIME OPTIONS(description='End date time for treatment'),
  history_user_name STRING OPTIONS(description='Name of the user who updated record'),
  history_date_time DATETIME OPTIONS(description='Date time when record was updated'),
  log_id INT64 OPTIONS(description='Unique identifier for the log'),
  run_id INT64 OPTIONS(description='Identifier for last run'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY site_sk, source_patient_plan_id
OPTIONS(
  description='Contains information of plans linked to course involved in Radiation Oncology'
);
