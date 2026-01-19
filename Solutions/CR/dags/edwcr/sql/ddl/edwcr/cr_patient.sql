CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cr_patient
(
  cr_patient_id INT64 NOT NULL OPTIONS(description='A unique identifier for each patient.'),
  patient_gender_id INT64 OPTIONS(description='Gender Identifier of the patient'),
  patient_race_id INT64 OPTIONS(description='Racial Identifier of the patient'),
  vital_status_id INT64 OPTIONS(description='Identifier indicating vital status of patient'),
  patient_system_id INT64 OPTIONS(description='System Identifier for a patient'),
  coid STRING NOT NULL OPTIONS(description='A five character code for each facility.'),
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  patient_birth_date DATE OPTIONS(description='Brith date of the patient'),
  last_contact_date DATE OPTIONS(description='Date of last contact for patient record'),
  patient_first_name STRING OPTIONS(description='First name of the patient'),
  patient_middle_name STRING OPTIONS(description='Middle name of the patient'),
  patient_last_name STRING OPTIONS(description='Last name of the patient'),
  patient_email_address_text STRING OPTIONS(description='Email Address of the patient'),
  accession_num_code STRING OPTIONS(description='Patient identification by a permanent nine-digit accession number and each of the patient primary tumors is identified by a different two-digit sequence number'),
  patient_market_urn_text STRING OPTIONS(description='The market universal record number for a patient.'),
  medical_record_num STRING OPTIONS(description='The medical record number for a patient.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY cr_patient_id
OPTIONS(
  description='Contains all the patients navigated by Sarah Cannon through Metriq'
);
