CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_rad_onc_diagnosis_code
(
  diagnosis_code_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for a diagnosis code in EDW'),
  site_sk INT64 NOT NULL OPTIONS(description='Unique identifier for a site of Radiation oncology'),
  source_diagnosis_code_id INT64 NOT NULL OPTIONS(description='Unique identifier for a diagnosis code in Radiation Oncology'),
  diagnosis_code STRING OPTIONS(description='Diagnosis code for radiation oncology'),
  diagnosis_site_text STRING OPTIONS(description='Text for site of diagnosis'),
  diagnosis_code_class_schema_id INT64 OPTIONS(description='Identifier for diagnosis class code'),
  diagnosis_clinical_desc STRING OPTIONS(description='Description for diagnosis clinical description'),
  diagnosis_long_desc STRING OPTIONS(description='Long description for diagnosis describing the disease'),
  diagnosis_type_code STRING OPTIONS(description='Type of diagnosis code i.e. ICD-10-AM,ICD-10-CM etc.'),
  log_id INT64 OPTIONS(description='Unique identifier for the log'),
  run_id INT64 OPTIONS(description='Identifier for last run'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY site_sk, source_diagnosis_code_id
OPTIONS(
  description='Contains information for radiation oncology diagnosis code'
);
