CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.rad_onc_patient
(
  patient_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for each patient in EDW'),
  patient_address_sk INT64 OPTIONS(description='Unique surrogate key for patients address'),
  site_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for a site in EDW'),
  source_patient_id INT64 NOT NULL OPTIONS(description='Unique identifier for a patient in Radiation Oncology in source'),
  medical_record_num STRING NOT NULL OPTIONS(description='The medical record number for a patient.'),
  patient_birth_date_time DATETIME OPTIONS(description='Brith date time of the patient'),
  patient_first_name STRING OPTIONS(description='First name of the patient'),
  patient_middle_name STRING OPTIONS(description='Middle name of the patient'),
  patient_last_name STRING OPTIONS(description='Last name of the patient'),
  patient_title_name STRING OPTIONS(description='Title name of the patient'),
  patient_email_address_text STRING OPTIONS(description='Email Address of the patient'),
  patient_in_out_ind STRING OPTIONS(description='Indicator for In and Out Patient. i.e I for Inpatient , O for Out Patient and U for others'),
  patient_death_ind STRING OPTIONS(description='Indicator where patient is dead or alive.'),
  patient_death_date DATE OPTIONS(description='Date of patients death'),
  patient_death_reason_text STRING OPTIONS(description='Text for patients death'),
  clinical_trial_ind STRING OPTIONS(description='Indicates if patient had clinical trial or not'),
  patient_transportation_text STRING OPTIONS(description='Text for transpotaion of patient'),
  patient_global_unique_id_text STRING OPTIONS(description='Unique identifier for patient '),
  patient_room_number_text STRING OPTIONS(description='Text for patient room number'),
  active_ind STRING OPTIONS(description='Indicates if patient record is active or not'),
  patient_language_text STRING OPTIONS(description='Text for language used by patient'),
  patient_notes_text STRING OPTIONS(description='Text for patient notes'),
  log_id INT64 OPTIONS(description='Unique identifier for the log'),
  run_id INT64 OPTIONS(description='Identifier for last run'),
  history_user_name STRING OPTIONS(description='Name of the user who updated record'),
  history_date_time DATETIME OPTIONS(description='Date time when record was updated'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY site_sk, source_patient_id
OPTIONS(
  description='Contains information of patients underwent Radiation Oncology'
);
