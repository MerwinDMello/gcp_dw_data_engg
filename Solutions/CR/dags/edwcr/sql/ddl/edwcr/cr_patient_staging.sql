CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cr_patient_staging
(
  cr_patient_staging_sid INT64 NOT NULL OPTIONS(description='Unique record generated for each cancer stage of the patient'),
  cr_patient_id INT64 OPTIONS(description='A unique identifier for each patient.'),
  tumor_id INT64 OPTIONS(description='Unique identifier of tumor'),
  ajcc_stage_id INT64 OPTIONS(description='Stage using current American Joint Committee on Cancer staging (determined via physical exam, imaging, labs,  biopsy)'),
  cancer_stage_classification_method_code STRING OPTIONS(description='This column tells if a cancer stage is classified using Clinical or Pathology method'),
  cancer_stage_type_code STRING OPTIONS(description='Stage type of cancer'),
  cancer_stage_result_text STRING OPTIONS(description='This column gives the details of a particular cancer stage'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY cr_patient_staging_sid
OPTIONS(
  description='This table contains tumor staging details of patient'
);
