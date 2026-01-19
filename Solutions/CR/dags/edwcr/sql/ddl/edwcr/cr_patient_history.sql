CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cr_patient_history
(
  cr_patient_id INT64 NOT NULL OPTIONS(description='A unique identifier for each patient.'),
  tumor_id INT64 NOT NULL OPTIONS(description='The first line of the address of the patient.'),
  smoked_history_id INT64 OPTIONS(description='Identifier for patient smoking history'),
  tobacco_amt_text STRING OPTIONS(description='Amount of Tobacco used daily'),
  years_tobacco_used_num_text STRING OPTIONS(description='Years of tobacco used'),
  family_cancer_history_type_id INT64 OPTIONS(description='Cancer history type of the family member'),
  patient_cancer_history_type_id INT64 OPTIONS(description='Previous cancer type of patient'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY cr_patient_id, tumor_id
OPTIONS(
  description='Contains the patient and his/her family cancer history'
);
