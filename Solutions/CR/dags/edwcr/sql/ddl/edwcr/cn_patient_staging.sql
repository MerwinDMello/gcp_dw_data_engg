CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cn_patient_staging
(
  cn_patient_staging_sid INT64 NOT NULL OPTIONS(description='A unique identifier for each record.'),
  cancer_stage_class_method_code STRING NOT NULL OPTIONS(description='Identifies if the staging refers to pathological staging or clinical staging.'),
  cancer_staging_type_code STRING OPTIONS(description='Indicates if the patient stage either T , M , N'),
  diagnosis_result_id INT64 OPTIONS(description='A unique identifier for each diagnosis result.'),
  nav_patient_id NUMERIC(29) OPTIONS(description='A unique identifier for each patient.'),
  tumor_type_id INT64 OPTIONS(description='A unique identifier for each tumor type.'),
  nav_diagnosis_id INT64 OPTIONS(description='A unique identfier for each diagnosis type.'),
  navigator_id INT64 OPTIONS(description='A unique identifier for each navigator.'),
  coid STRING NOT NULL OPTIONS(description='A five character code for each facility.'),
  company_code STRING OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  cancer_staging_result_code STRING OPTIONS(description='Indicates the result value for each staging type per patient.'),
  cancer_stage_code STRING OPTIONS(description='Clinical or Pathalogical Cancer stage code'),
  hashbite_ssk STRING OPTIONS(description='A unique guid that ties the record to the source system.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY diagnosis_result_id, nav_patient_id, tumor_type_id, nav_diagnosis_id
OPTIONS(
  description='Contains the details behind a patients level of cancer broken down into different stages.'
);
