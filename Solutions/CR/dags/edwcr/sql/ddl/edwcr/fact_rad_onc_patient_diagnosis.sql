CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.fact_rad_onc_patient_diagnosis
(
  fact_patient_diagnosis_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for fact data for patient diagnosis in EDW'),
  diagnosis_code_sk INT64 OPTIONS(description='Unique surrogate key generated for a diagnosis code in EDW'),
  patient_sk INT64 OPTIONS(description='Unique surrogate key generated for each patient in EDW'),
  diagnosis_status_id INT64 OPTIONS(description='Identifier for diagnosis status'),
  cell_category_id INT64 OPTIONS(description='Identifier for cell category'),
  cell_grade_id INT64 OPTIONS(description='Identifier for cell grade'),
  laterality_id INT64 OPTIONS(description='Identifier for laterality'),
  stage_id INT64 OPTIONS(description='Identifier for stage'),
  stage_status_id INT64 OPTIONS(description='Identifier for stage status'),
  recurrence_id INT64 OPTIONS(description='Identifier for recurrence'),
  invasion_id INT64 OPTIONS(description='Identifier for invasion'),
  confirmed_diagnosis_id INT64 OPTIONS(description='Identifier for confimed diagnosis'),
  diagnosis_type_id INT64 OPTIONS(description='Identifier for diagnosis type'),
  site_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for a site in EDW'),
  source_fact_patient_diagnosis_id INT64 NOT NULL OPTIONS(description='Unique identifier for patient diagnosis fact data in Radiation Oncology in source'),
  diagnosis_status_date DATE OPTIONS(description='Date for the patient diagnosis'),
  diagnosis_text STRING OPTIONS(description='Text for diagnosis conducted'),
  clinical_text STRING OPTIONS(description='Text for clinical description'),
  pathology_comment_text STRING OPTIONS(description='Text for pathology comments'),
  node_num INT64 OPTIONS(description='Number of nodes'),
  positive_node_num INT64 OPTIONS(description='Number of positive nodes'),
  log_id INT64 OPTIONS(description='Unique identifier for the log'),
  run_id INT64 OPTIONS(description='Identifier for last run'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY site_sk, source_fact_patient_diagnosis_id
OPTIONS(
  description='Contains fact information of Radiation Oncology for a patient diagnosis'
);
