CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cdm_patient_history
(
  clinical_patient_query_sid NUMERIC(29) NOT NULL OPTIONS(description='System identifier to uniquely identify a patient query in teh system.'),
  patient_dw_id NUMERIC(29) NOT NULL OPTIONS(description='A system generated number used to uniquely identify a patient.'),
  coid STRING NOT NULL OPTIONS(description='The company identifier which uniquely identifies a facility to corporate and all other facilities.'),
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  pat_acct_num NUMERIC(29) NOT NULL OPTIONS(description='A unique number assigned by the hospital to the patient at time of registration.'),
  smoking_status_id INT64 OPTIONS(description='Number to identify the smoking status of the patient. '),
  smoking_status_desc STRING OPTIONS(description='The Smoking Status Description'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY patient_dw_id
OPTIONS(
  description='Contains the information about the patient smoking status.'
);
