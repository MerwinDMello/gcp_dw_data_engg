CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_therapy_type
(
  therapy_type_id INT64 NOT NULL OPTIONS(description='A unique identifier for each therapy type.'),
  therapy_type_desc STRING OPTIONS(description='The description for each unique therapy type.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY therapy_type_id
OPTIONS(
  description='Contains a distinct list of therapy types.'
);
