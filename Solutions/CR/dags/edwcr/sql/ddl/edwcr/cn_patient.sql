CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cn_patient
(
  nav_patient_id NUMERIC(29) NOT NULL OPTIONS(description='A unique identifier for each patient.'),
  navigator_id INT64 OPTIONS(description='A unique identifier for each navigator.'),
  gynecologist_physician_id INT64 OPTIONS(description='The physician identifier for the gynecologist.'),
  primary_care_physician_id INT64 OPTIONS(description='A unique identifier for the primary care physician.'),
  coid STRING NOT NULL OPTIONS(description='A five character code for each facility.'),
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  patient_market_urn STRING OPTIONS(description='The market universal record number for a patient.'),
  medical_record_num STRING OPTIONS(description='The medical record number for a patient.'),
  empi_text STRING OPTIONS(description='A enterprise identifier for a person that can be used to track a patient across care settings.'),
  facility_mnemonic_cs STRING OPTIONS(description='Mnemonic that is associated to the sending facility of the message'),
  network_mnemonic_cs STRING OPTIONS(description='The network mnemonic for the facility in which the patient visited.'),
  nav_create_date DATE OPTIONS(description='The date the patient was first created in iNavigate.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY nav_patient_id
OPTIONS(
  description='Contains all the patients navigated by Sarah Cannon'
);
