CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cancer_patient_detail
(
  cancer_patient_detail_sk NUMERIC(29) NOT NULL OPTIONS(description='Unique key generated for consolidated patient detail'),
  cancer_patient_driver_sk NUMERIC(29) OPTIONS(description='Unique key generated for a consolidated record'),
  cr_patient_id INT64 OPTIONS(description='A unique identifier for each patient in Cancer Registry'),
  cn_patient_id NUMERIC(29) OPTIONS(description='A unique identifier for each patient in inavigate'),
  cp_patient_id NUMERIC(29) OPTIONS(description='A unique identifier for each patient in Cancer Patient ID system'),
  coid STRING NOT NULL OPTIONS(description='A five character code for each facility.'),
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  network_mnemonic_cs STRING OPTIONS(description='Network Mnemonic code'),
  patient_market_urn_text STRING OPTIONS(description='The market universal record number for a patient.'),
  medical_record_num STRING OPTIONS(description='The medical record number for a patient.'),
  relationship_name STRING OPTIONS(description='Relation with the patient'),
  address_line_1_text STRING OPTIONS(description='Text containing Patient Address first line'),
  address_line_2_text STRING OPTIONS(description='Text containing Patient Address second line'),
  city_name STRING OPTIONS(description='Name of the city where patient lives'),
  death_date DATE OPTIONS(description='Death date of patient'),
  patient_birth_date DATE OPTIONS(description='Brith date of the patient'),
  patient_email_address_text STRING OPTIONS(description='Email Address of the patient'),
  patient_gender_code STRING OPTIONS(description='Gender code of the patient'),
  patient_state_code STRING OPTIONS(description='State code of the patient'),
  zip_code STRING OPTIONS(description='Zip code of patient'),
  phone_num_type_code STRING OPTIONS(description='Type of phone number i.e. cellphone or homephone'),
  phone_num STRING OPTIONS(description='Phone number of patient or his/her contact'),
  preferred_language_text STRING OPTIONS(description='Preferred language of the patient'),
  insurance_type_name STRING OPTIONS(description='Type of Insurance'),
  preferred_contact_method_text STRING OPTIONS(description='Preferred method of contact for the patient'),
  insurance_company_name STRING OPTIONS(description='Insurance company for the patient'),
  race_name STRING OPTIONS(description='Name of the race of patient'),
  patient_system_id INT64 OPTIONS(description='System Identifier for a patient'),
  vital_status_name STRING OPTIONS(description='Vital status of patient'),
  source_system_code STRING OPTIONS(description='Unique value to identify the system from which data is sourced'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY cancer_patient_detail_sk
OPTIONS(
  description='Contains details of consolidated patients navigated by Sarah Cannon through Metriq iNavigate and Cancer Patient ID'
);
