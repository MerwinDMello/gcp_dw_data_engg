CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cn_patient_phone_num
(
  nav_patient_id NUMERIC(29) NOT NULL OPTIONS(description='A unique identifier for each patient.'),
  phone_num_type_code STRING NOT NULL OPTIONS(description='Identifies the type of phone number - home, cell, work.'),
  phone_num STRING OPTIONS(description='The phone number of the patient.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY nav_patient_id, phone_num_type_code
OPTIONS(
  description='Contains the different phone numbers a patient could be associated with.'
);
