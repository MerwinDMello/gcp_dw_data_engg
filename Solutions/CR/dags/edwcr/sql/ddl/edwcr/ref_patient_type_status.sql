CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_patient_type_status
(
  patient_type_status_sk INT64 NOT NULL OPTIONS(description='Unique key generated for status of patient type'),
  patient_type_status_code STRING OPTIONS(description='Code for Patient type status'),
  patient_type_status_desc STRING OPTIONS(description='Decription for patient type status'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY patient_type_status_sk
OPTIONS(
  description='This table will store the patient type status for cancer patient Id'
);
