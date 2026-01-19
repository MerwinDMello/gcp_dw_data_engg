CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_test_type
(
  test_type_id INT64 NOT NULL OPTIONS(description='A unique identifier for each test type.'),
  test_type_desc STRING OPTIONS(description='The description for each test type.'),
  test_sub_type_desc STRING OPTIONS(description='The description for each test sub type.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY test_type_id
OPTIONS(
  description='Contains the distinct test types for the patients'
);
