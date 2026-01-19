CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.fact_rad_onc_patient
(
  fact_patient_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for fact data for patient  in EDW'),
  hospital_sk INT64 OPTIONS(description='Unique surrogate key generated for a radiation oncology hospital data in EDW'),
  patient_sk INT64 OPTIONS(description='Unique surrogate key generated for each patient in EDW'),
  patient_status_id INT64 OPTIONS(description='Identifier for patient status'),
  location_sk INT64 OPTIONS(description='Surrogate key for the location'),
  race_id INT64 OPTIONS(description='Identifier for race of patient'),
  gender_id INT64 OPTIONS(description='Identifier for gender of patient'),
  site_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for a site in EDW'),
  source_fact_patient_id INT64 NOT NULL OPTIONS(description='Unique identifier for patient fact data in Radiation Oncology in source'),
  creation_date_time DATETIME OPTIONS(description='Date time for the patient creation'),
  admission_date_time DATETIME OPTIONS(description='Date time for admission of patient'),
  discharge_date_time DATETIME OPTIONS(description='Date time for discharge of patient'),
  log_id INT64 OPTIONS(description='Unique identifier for the log'),
  run_id INT64 OPTIONS(description='Identifier for last run'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY site_sk, source_fact_patient_id
OPTIONS(
  description='Contains fact information of Radiation Oncology for a patient'
);
