CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_result
(
  nav_result_id INT64 NOT NULL OPTIONS(description='A unique identifier for each result.'),
  nav_result_desc STRING OPTIONS(description='The result text field that details results for different tests.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY nav_result_id
OPTIONS(
  description='Contains surgery, biopsy result, diagnosis and other oncology navigation result possibilities.'
);
