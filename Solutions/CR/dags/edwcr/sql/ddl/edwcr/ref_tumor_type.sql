CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_tumor_type
(
  tumor_type_id INT64 NOT NULL OPTIONS(description='A unique identifier for each tumor type.'),
  tumor_type_desc STRING OPTIONS(description='Description of the tumor type.'),
  navigation_tumor_code_id INT64 OPTIONS(description='A unique identifier for the tumor.'),
  tumor_type_group_name STRING OPTIONS(description='A roll-up of each tumor type.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY tumor_type_id
OPTIONS(
  description='Reference for the type of tumor'
);
