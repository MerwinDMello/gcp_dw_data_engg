CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cn_person
(
  nav_patient_id NUMERIC(29) NOT NULL OPTIONS(description='A unique identifier for each patient.'),
  birth_date DATE OPTIONS(description='The birth date of the patient.'),
  first_name STRING OPTIONS(description='The first name of the patient.'),
  last_name STRING OPTIONS(description='The persons last name.'),
  middle_name STRING OPTIONS(description='The middle name of the person.'),
  preferred_name STRING OPTIONS(description='The perferred name of the patient.'),
  gender_code STRING OPTIONS(description='A code identifying the patient gender.'),
  preferred_language_text STRING OPTIONS(description='The perferred language of the patient.'),
  death_date DATE OPTIONS(description='The date the patient died.'),
  patient_email_text STRING OPTIONS(description='The email of the patient.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY nav_patient_id
OPTIONS(
  description='Contains all the demographic data for each person navigated by Sarah Cannon'
);
