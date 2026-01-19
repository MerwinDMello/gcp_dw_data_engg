CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_physician_specialty
(
  physician_specialty_id INT64 NOT NULL OPTIONS(description='A unique identifier for each specialty for a physician.'),
  physician_specialty_desc STRING OPTIONS(description='The specialty description for the physician.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY physician_specialty_id
OPTIONS(
  description='Contains the list of physician specialties.'
);
