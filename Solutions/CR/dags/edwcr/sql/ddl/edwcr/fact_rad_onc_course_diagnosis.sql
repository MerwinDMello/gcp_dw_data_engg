CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.fact_rad_onc_course_diagnosis
(
  fact_course_diagnosis_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for fact data for course diagnosis in EDW'),
  fact_patient_diagnosis_sk INT64 OPTIONS(description='Unique surrogate key generated for fact data for patient diagnosis in EDW'),
  patient_course_sk INT64 OPTIONS(description='Unique surrogate key generated for each patient course in EDW'),
  diagnosis_code_sk INT64 OPTIONS(description='Unique surrogate key generated for a diagnosis code in EDW'),
  site_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for a site in EDW'),
  source_fact_course_diagnosis_id INT64 NOT NULL OPTIONS(description='Unique identifier for course diagnosis fact data in Radiation Oncology in source'),
  primary_course_ind STRING OPTIONS(description='Indicates if course taken is primary or not'),
  log_id INT64 OPTIONS(description='Unique identifier for the log'),
  run_id INT64 OPTIONS(description='Identifier for last run'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY site_sk, source_fact_course_diagnosis_id
OPTIONS(
  description='Contains fact information of Radiation Oncology for a course diagnosis'
);
