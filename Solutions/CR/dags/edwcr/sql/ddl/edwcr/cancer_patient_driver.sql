CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cancer_patient_driver
(
  cancer_patient_driver_sk NUMERIC(29) NOT NULL OPTIONS(description='Unique key generated for consolidated patient records'),
  cr_patient_id INT64 OPTIONS(description='A unique identifier for each patient in Cancer Registry'),
  cp_patient_id NUMERIC(29) OPTIONS(description='A unique identifier for each patient in Cancer Patient ID system'),
  cn_patient_id NUMERIC(29) OPTIONS(description='A unique identifier for each patient in inavigate'),
  coid STRING NOT NULL OPTIONS(description='A five character code for each facility.'),
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  network_mnemonic_cs STRING OPTIONS(description='Network mnemonic code'),
  patient_market_urn_text STRING OPTIONS(description='The market universal record number for a patient.'),
  medical_record_num STRING OPTIONS(description='The medical record number for a patient.'),
  empi_text STRING OPTIONS(description='This is the unique Enterprise Master Person Index number assigned by Initiate to uniquely identify the patient across clinical systems MEDITECH, Epic, eCW, etc.'),
  patient_first_name STRING OPTIONS(description='First name of the patient'),
  patient_middle_name STRING OPTIONS(description='Middle name of the patient'),
  patient_last_name STRING OPTIONS(description='Last name of the patient'),
  preferred_name STRING OPTIONS(description='Preferred name of the patient'),
  source_system_code STRING OPTIONS(description='Unique value to identify the system from which data is sourced'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY cancer_patient_driver_sk
OPTIONS(
  description='Contains consolidated patients navigated by Sarah Cannon through Metriq iNavigate and Cancer Patient ID.This will be the driver table for reporting.'
);
