CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_diagnosis_navigation
(
  nav_diagnosis_id INT64 NOT NULL OPTIONS(description='A unique identfier for each diagnosis type.'),
  nav_diagnosis_desc STRING OPTIONS(description='The description for each diagnosis type.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY nav_diagnosis_id
OPTIONS(
  description='Contains a distinct list of diagnoses.'
);
