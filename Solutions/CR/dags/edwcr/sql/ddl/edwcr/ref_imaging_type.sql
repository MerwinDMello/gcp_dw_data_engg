CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_imaging_type
(
  imaging_type_id INT64 NOT NULL OPTIONS(description='A unique identifier for each image type.'),
  imaging_type_desc STRING OPTIONS(description='The different types of imaging.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY imaging_type_id
OPTIONS(
  description='Contains a distinct list of imaging types.'
);
