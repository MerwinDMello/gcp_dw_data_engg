CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cr_patient_diagnosis_detail
(
  tumor_id INT64 NOT NULL OPTIONS(description='Unique identifier of tumor'),
  cr_patient_id INT64 NOT NULL OPTIONS(description='A unique identifier for each patient.'),
  tumor_site_id INT64 OPTIONS(description='Unique identifier of tumor site'),
  diagnosis_name_id INT64 OPTIONS(description='Identifier for name of diagnosis'),
  diagnosis_date DATE OPTIONS(description='Date of diagnosis'),
  diagnose_age_num INT64 OPTIONS(description='Age at Diagnosis of tumor'),
  first_diagnose_year_num INT64 OPTIONS(description='Year tumor was first diagnosed'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY tumor_id, cr_patient_id
OPTIONS(
  description='This table contains diagnosis details of patient'
);
