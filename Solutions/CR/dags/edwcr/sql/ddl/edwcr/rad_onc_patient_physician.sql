CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.rad_onc_patient_physician
(
  patient_physician_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for each physician linked to patient  in EDW'),
  site_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for a site in EDW'),
  source_patient_physician_id INT64 NOT NULL OPTIONS(description='Unique identifier for a physician in Radiation Oncology in source for patient'),
  patient_sk INT64 OPTIONS(description='Unique surrogate key generated for each patient in EDW'),
  primary_physician_ind STRING OPTIONS(description='Indicator which shows this is primary physician for patient'),
  oncologist_ind STRING OPTIONS(description='Indicates whether physician is oncologist or not'),
  resource_id INT64 OPTIONS(description='Internal Identification for Aria Resource (physician/staff/machine)'),
  log_id INT64 OPTIONS(description='Unique identifier for the log'),
  run_id INT64 OPTIONS(description='Identifier for last run'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY site_sk, source_patient_physician_id
OPTIONS(
  description='Contains information of physician involved in Radiation Oncology for a patient'
);
